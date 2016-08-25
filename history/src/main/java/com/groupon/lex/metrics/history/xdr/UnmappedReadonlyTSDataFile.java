package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.history.TSData;
import static com.groupon.lex.metrics.history.xdr.Const.version_major;
import static com.groupon.lex.metrics.history.xdr.Const.version_minor;
import com.groupon.lex.metrics.history.xdr.support.GzipDecodingBufferSupplier;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID1_EXPECT;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID2_EXPECT;
import com.groupon.lex.metrics.history.xdr.support.Parser;
import com.groupon.lex.metrics.history.xdr.support.XdrStreamIterator;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static java.util.Collections.emptyIterator;
import java.util.Iterator;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acplt.oncrpc.OncRpcException;
import org.dcache.xdr.XdrBufferDecodingStream;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public class UnmappedReadonlyTSDataFile implements TSData {
    private static final Logger LOG = Logger.getLogger(UnmappedReadonlyTSDataFile.class.getName());
    private final GCCloseable<FileChannel> fd_;
    private final DateTime begin_, end_;
    private final int version_;
    private final boolean is_gzipped_;

    public UnmappedReadonlyTSDataFile(GCCloseable<FileChannel> fd) throws IOException {
        fd_ = requireNonNull(fd);

        final ByteBuffer gzip_detection_buf = ByteBuffer.allocate(2);
        int gzip_detection_buf_len;
        gzip_detection_buf_len = fd.get().read(gzip_detection_buf, 0);
        if (gzip_detection_buf_len >= 2) {
            gzip_detection_buf.flip();
            final byte id1 = gzip_detection_buf.get();
            final byte id2 = gzip_detection_buf.get();
            is_gzipped_ = (id1 == ID1_EXPECT && id2 == ID2_EXPECT);
        } else {
            is_gzipped_ = false;
        }
        LOG.log(Level.INFO, "is_gzipped: {0}", is_gzipped_);

        final XdrBufferDecodingStream stream;
        if (is_gzipped_)
            stream = new XdrBufferDecodingStream(new GzipDecodingBufferSupplier(new UnmappedBufferSupplier()));
        else
            stream = new XdrBufferDecodingStream(new UnmappedBufferSupplier());

        final tsfile_mimeheader mimeheader;
        try {
            mimeheader = new tsfile_mimeheader(stream);
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }
        version_ = Const.validateHeaderOrThrow(mimeheader);

        final Parser.BeginEnd header = Parser.fromVersion(version_).header(stream);
        begin_ = header.getBegin();
        end_ = header.getEnd();
        LOG.log(Level.INFO, "instantiated: version={0}.{1} begin={2}, end={3}", new Object[]{version_major(version_), version_minor(version_), begin_, end_});
    }

    public static UnmappedReadonlyTSDataFile open(Path file) throws IOException {
        final GCCloseable<FileChannel> fd = new GCCloseable<>(FileChannel.open(file, StandardOpenOption.READ));
        return new UnmappedReadonlyTSDataFile(fd);
    }

    @Override
    public DateTime getBegin() { return begin_; }
    @Override
    public DateTime getEnd() { return end_; }
    @Override
    public short getMajor() { return version_major(version_); }
    @Override
    public short getMinor() { return version_minor(version_); }

    @Override
    public long getFileSize() {
        try {
            return fd_.get().size();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "unable to get file size", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean isGzipped() { return is_gzipped_; }

    private class UnmappedBufferSupplier implements BufferSupplier {
        private long offset_;

        public UnmappedBufferSupplier() {
            offset_ = 0;
        }

        @Override
        public void load(ByteBuffer buf) throws IOException {
            offset_ += fd_.get().read(buf, offset_);
        }

        @Override
        public boolean atEof() throws IOException {
            return offset_ == fd_.get().size();
        }
    }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        try {
            BufferSupplier decoder = new UnmappedBufferSupplier();
            if (is_gzipped_) {
                decoder = new GzipDecodingBufferSupplier(decoder);
                return new XdrStreamIterator(decoder);
            } else {
                return new XdrStreamIterator(ByteBuffer.allocateDirect(1024 * 1024), decoder);
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "unable to create iterator", ex);
            return emptyIterator();
        }
    }

    @Override
    public boolean add(TimeSeriesCollection tsv) {
        throw new UnsupportedOperationException("add");
    }

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.of(fd_);
    }
}
