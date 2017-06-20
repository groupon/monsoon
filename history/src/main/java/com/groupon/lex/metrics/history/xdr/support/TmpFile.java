package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.history.xdr.support.writer.CloseInhibitingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.FileWriter;
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
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;

public class TmpFile<T extends XdrAble> implements Closeable {
    private static final Logger LOG = Logger.getLogger(TmpFile.class.getName());
    private final FileChannel fd;
    private final Compression compression;
    private Optional<FileWriter> fileWriter;
    private int count = 0;

    private TmpFile(FileChannel fd, Compression compression) throws IOException {
        this.fd = fd;
        this.compression = compression;
        this.fileWriter = Optional.of(compression.wrap(new FileChannelWriter(fd, 0), false));
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
                fileWriter.get().close();
                fileWriter = Optional.empty();
            }
        } catch (IOException | RuntimeException ex) {
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
        try (final XdrEncodingFileWriter xdr = fileWriter
                .map(CloseInhibitingWriter::new)
                .map(XdrEncodingFileWriter::new)
                .orElseThrow(() -> new IllegalStateException("cannot write: we switched to reading"))) {
            xdr.beginEncoding();
            v.xdrEncode(xdr);
            xdr.endEncoding();
            ++count;
        }
    }

    public synchronized int size() {
        return count;
    }

    public Iterator<T> iterator(@NonNull Supplier<T> valueSupplier) throws IOException, OncRpcException {
        synchronized (this) {
            if (fileWriter.isPresent()) {
                // Flush all data to the file and prevent any future writes.
                fileWriter.get().close();
                fileWriter = Optional.empty();
            }
        }

        return new IteratorImpl<>(new XdrDecodingFileReader(compression.wrap(new FileChannelReader(fd, 0))), valueSupplier, count);
    }

    @RequiredArgsConstructor
    private static class IteratorImpl<T extends XdrAble> implements Iterator<T> {
        @NonNull
        private final XdrDecodingFileReader reader;
        @NonNull
        private final Supplier<T> valueSupplier;
        private final int maxCount;
        private int count = 0;

        @Override
        public boolean hasNext() {
            return count < maxCount;
        }

        @Override
        public T next() {
            try {
                if (count == maxCount)
                    throw new NoSuchElementException("cannot read: at end of file");
                T v = readValue();
                ++count;

                return v;
            } catch (IOException | OncRpcException ex) {
                LOG.log(Level.WARNING, "decoding error for " + this, ex);
                throw new DecodingException("cannot read: decoding failed", ex);
            }
        }

        private T readValue() throws OncRpcException, IOException {
            final T v = valueSupplier.get();
            reader.beginDecoding();
            try {
                v.xdrDecode(reader);
            } finally {
                reader.endDecoding();
            }
            return v;
        }
    }
}
