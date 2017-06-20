package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;

public class TmpFile<T extends XdrAble> implements Closeable {
    private static final Logger LOG = Logger.getLogger(TmpFile.class.getName());
    private static final int BUFSIZ = 1024;
    private final FileChannel fd;
    private final Compression compression;
    private Optional<XdrEncodingFileWriter> fileWriter;
    private int count = 0;

    private TmpFile(FileChannel fd, Compression compression) throws IOException {
        this.fd = fd;
        this.compression = compression;
        this.fileWriter = Optional.of(new XdrEncodingFileWriter(compression.wrap(new FileChannelWriter(fd, 0), false), BUFSIZ));

        try {
            this.fileWriter.get().beginEncoding();
        } catch (OncRpcException ex) {
            try {
                this.fd.close();
            } catch (Exception ex1) {
                ex.addSuppressed(ex1);
            }
            throw new IOException("XDR initialization failed", ex);
        }
    }

    public TmpFile(Path dir, Compression compression) throws IOException {
        this(FileUtil.createTempFile(dir, "monsoon", ".tmp"), compression);
    }

    public TmpFile(Compression compression) throws IOException {
        this(FileUtil.createTempFile("monsoon", ".tmp"), compression);
    }

    @Override
    public synchronized void close() throws IOException {
        Exception exc = null;  // We capture reader/writer errors, but don't throw them.
        try {
            if (fileWriter.isPresent()) {
                fileWriter.get().endEncoding();
                fileWriter.get().close();
                fileWriter = Optional.empty();
            }
        } catch (OncRpcException | IOException | RuntimeException ex) {
            exc = ex;
        }

        try {
            fd.close();
        } catch (IOException | RuntimeException ex) {
            ex.addSuppressed(exc);  // We do add reader/writer exception to the IOException, as they're useful in debugging.
            throw ex;
        }
    }

    public synchronized void add(@NonNull T v) throws OncRpcException, IOException {
        final XdrEncodingFileWriter xdr = fileWriter.orElseThrow(() -> new IllegalStateException("cannot write: we switched to reading"));
        v.xdrEncode(xdr);
        ++count;
    }

    public synchronized int size() {
        return count;
    }

    public Iterator<T> iterator(@NonNull Supplier<T> valueSupplier) throws IOException, OncRpcException {
        synchronized (this) {
            if (fileWriter.isPresent()) {
                // Flush all data to the file and prevent any future writes.
                fileWriter.get().endEncoding();
                fileWriter.get().close();
                fileWriter = Optional.empty();
            }
        }

        return new IteratorImpl<>(compression.wrap(new FileChannelReader(fd, 0)), BUFSIZ, valueSupplier, count);
    }

    private static class IteratorImpl<T extends XdrAble> implements Iterator<T> {
        private final XdrDecodingFileReader xdr;
        private final Supplier<T> valueSupplier;
        private final int maxCount;
        private int count = 0;

        public IteratorImpl(@NonNull FileReader reader, int bufsiz, Supplier<T> valueSupplier, int maxCount) {
            this.xdr = new XdrDecodingFileReader(reader, bufsiz);
            this.valueSupplier = valueSupplier;
            this.maxCount = maxCount;

            this.xdr.beginDecoding();
        }

        @Override
        public boolean hasNext() {
            return count < maxCount;
        }

        @Override
        public T next() {
            try {
                if (count == maxCount)
                    throw new NoSuchElementException("cannot read: at end of file");
                T v = valueSupplier.get();
                v.xdrDecode(xdr);
                ++count;
                if (count == maxCount) {
                    xdr.endDecoding();
                    xdr.close();
                }
                return v;
            } catch (IOException | OncRpcException ex) {
                LOG.log(Level.WARNING, "decoding error for " + this, ex);
                throw new DecodingException("cannot read: decoding failed", ex);
            }
        }
    }
}
