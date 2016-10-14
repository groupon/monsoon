package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.lib.Any2;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;

public class TmpFile<T extends XdrAble> implements Closeable {
    private final FileChannel fd;
    private final Compression compression;
    private Any2<XdrDecodingFileReader, XdrEncodingFileWriter> fileIO;
    private int count = 0;

    private TmpFile(FileChannel fd, Compression compression) throws IOException {
        this.fd = fd;
        this.compression = compression;
        this.fileIO = Any2.right(new XdrEncodingFileWriter(compression.wrap(new FileChannelWriter(fd, 0)), 64 * 1024));
    }

    public TmpFile(Path dir, Compression compression) throws IOException {
        this(FileUtil.createTempFile(dir, "monsoon", ".tmp"), compression);
    }

    public TmpFile(Compression compression) throws IOException {
        this(FileUtil.createTempFile("monsoon", ".tmp"), compression);
    }

    @Override
    public void close() throws IOException {
        Exception exc = null;  // We capture reader/writer errors, but don't throw them.
        try {
            AutoCloseable readerWriter = fileIO.mapCombine(x -> x, x -> x);
            if (readerWriter != null)
                readerWriter.close();
        } catch (Exception ex) {
            exc = ex;
        }

        try {
            fd.close();
        } catch (IOException ex) {
            ex.addSuppressed(exc);  // We do add reader/writer exception to the IOException, as they're useful in debugging.
            throw ex;
        }
    }

    public void add(@NonNull T v) throws OncRpcException, IOException {
        final XdrEncodingFileWriter xdr = fileIO.getRight().orElseThrow(() -> new IllegalStateException("cannot write: we switched to reading"));
        if (count == 0)
            xdr.beginEncoding();
        v.xdrEncode(xdr);
        ++count;
    }

    public int size() {
        return count;
    }

    public Iterator<T> iterator(@NonNull Supplier<T> valueSupplier) throws IOException, OncRpcException {
        if (!fileIO.getLeft().isPresent()) {
            fileIO.getRight().orElseThrow(IllegalStateException::new).endEncoding();
            fileIO.getRight().orElseThrow(IllegalStateException::new).close();
            fileIO = Any2.left(new XdrDecodingFileReader(compression.wrap(new FileChannelReader(fd, 0)), 64 * 1024));
        }

        final XdrDecodingFileReader xdr = fileIO.getLeft().orElseThrow(() -> new IllegalStateException("cannot read: expected reader"));
        xdr.beginDecoding();
        return new IteratorImpl(xdr, valueSupplier, count);
    }

    @RequiredArgsConstructor
    private class IteratorImpl implements Iterator<T> {
        private final XdrDecodingFileReader xdr;
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
                T v = valueSupplier.get();
                v.xdrDecode(xdr);
                ++count;
                if (count == maxCount) {
                    xdr.endDecoding();
                    xdr.close();
                }
                return v;
            } catch (IOException | OncRpcException ex) {
                throw new DecodingException("cannot read: decoding failed", ex);
            }
        }
    }
}
