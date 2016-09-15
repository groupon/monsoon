package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v1.xdr.FromXdr;
import com.groupon.lex.metrics.history.v1.xdr.ToXdr;
import com.groupon.lex.metrics.history.v1.xdr.tsfile_data;
import com.groupon.lex.metrics.history.v1.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.MAJOR;
import static com.groupon.lex.metrics.history.xdr.Const.MINOR;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrowForWrite;
import static com.groupon.lex.metrics.history.xdr.Const.writeMimeHeader;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import static java.lang.Math.max;
import static java.lang.Math.min;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Spliterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.acplt.oncrpc.OncRpcException;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferEncodingStream;
import lombok.Getter;
import org.joda.time.DateTime;

public final class WriteableTSDataFile implements TSData {
    private final GCCloseable<FileChannel> fd_;
    private SoftReference<TSData> readonly_ = new SoftReference<>(null);
    private final HeaderRegenerator header_regenerator_;
    private final FromXdr from_xdr_ = new FromXdr();
    @Getter
    private boolean ordered, unique;

    private static class PositionalReader implements BufferSupplier {
        private final GCCloseable<FileChannel> fd_;
        private long offset_ = 0;

        public PositionalReader(GCCloseable<FileChannel> fd) {
            fd_ = requireNonNull(fd);
        }

        @Override
        public void load(ByteBuffer buf) throws IOException {
            final int read = fd_.get().read(buf, offset_);
            if (read > 0)
                offset_ += read;
        }

        @Override
        public boolean atEof() throws IOException {
            return offset_ == fd_.get().size();
        }
    }

    private static class HeaderRegenerator {
        private final GCCloseable<FileChannel> fd_;
        private final tsfile_header hdr_;
        private final int hdr_size_;
        private boolean need_upgrade_;

        public HeaderRegenerator(GCCloseable<FileChannel> fd) throws IOException {
            fd_ = requireNonNull(fd);

            XdrBufferDecodingStream decoder = new XdrBufferDecodingStream(new PositionalReader(fd_));
            try {
                need_upgrade_ = validateHeaderOrThrowForWrite(decoder);
                hdr_ = new tsfile_header(decoder);
            } catch (OncRpcException ex) {
                throw new IOException(ex);
            }
            if (decoder.readBytes() > Integer.MAX_VALUE)
                throw new IllegalArgumentException("ehm, that's one big header...");
            hdr_size_ = (int)decoder.readBytes();
        }

        /**
         * Write all of a buffer.
         * Since FileChannel.write() doesn't guarantee all of the buffer will be written,
         * this function loops until all bytes are flushed out.
         */
        private void write_buffer_to_file_(List<ByteBuffer> bufs, long offset) throws IOException {
            final FileChannel fd = fd_.get();
            for (ByteBuffer buf : bufs) {
                while (buf.hasRemaining()) {
                    final int written = fd.write(buf, offset);
                    if (written < 0)
                        throw new IOException("unable to write");
                    offset += written;
                }
            }
        }

        private void rewrite_header_() throws IOException, OncRpcException {
            XdrBufferEncodingStream xdr = new XdrBufferEncodingStream(hdr_size_);
            xdr.beginEncoding();
            writeMimeHeader(xdr);
            hdr_.xdrEncode(xdr);
            xdr.endEncoding();

            if (xdr.getBuffersLength() != hdr_size_)
                throw new IllegalStateException("header was shorter than usual, I'm coming up " + (hdr_size_ - xdr.getBuffersLength()) + " bytes short");
            write_buffer_to_file_(xdr.getBuffers(), 0);
        }

        public void updateTimestamp(DateTime new_end_ts, Runnable onUnordered, Runnable onNotUnique) throws IOException {
            final long new_ts = ToXdr.timestamp(new_end_ts).value;

            if (hdr_.last.value < new_ts)
                onUnordered.run();
            if (hdr_.last.value == new_ts)
                onNotUnique.run();

            if (need_upgrade_ || hdr_.last.value < new_ts || hdr_.first.value > new_ts) {
                hdr_.last.value = max(hdr_.last.value, new_ts);
                hdr_.first.value = min(hdr_.first.value, new_ts);

                try {
                    rewrite_header_();
                } catch (OncRpcException ex) {
                    throw new IOException(ex);
                }
                need_upgrade_ = false;
            }
        }

        public DateTime getBegin() {
            return FromXdr.timestamp(hdr_.first);
        }

        public DateTime getEnd() {
            return FromXdr.timestamp(hdr_.last);
        }

        public short getMajor() {
            return MAJOR;
        }

        public short getMinor() {
            return MINOR;
        }
    }

    private WriteableTSDataFile(GCCloseable<FileChannel> fd) throws IOException {
        header_regenerator_ = new HeaderRegenerator(fd);  // Validates header.
        fd_ = requireNonNull(fd);

        /*
         * Since we keep a dictionary of previously seen items, we need to update
         * from_xdr_ with those dictionaries.
         * This means streaming the entire file through the xdr decoder.
         */
        XdrBufferDecodingStream decoder = new XdrBufferDecodingStream(new PositionalReader(fd_));
        try {
            new tsfile_mimeheader(decoder);  // Read MIME header and skip it.
            new tsfile_header(decoder);  // Read header and skip it.
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }
        final tsfile_data tsfd = new tsfile_data();
        try {
            while (!decoder.atEof()) {
                tsfd.xdrDecode(decoder);  // Load record.
                from_xdr_.data(tsfd);  // Update from_xdr, discard decoding result.
            }
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }

        /*
         * Check if the collection is ordered and unique.
         */
        final Iterator<TimeSeriesCollection> iter = iterator();
        if (!iter.hasNext()) {
            ordered = true;
            unique = true;
        } else {
            boolean uniqueLoopInvariant = true;
            boolean orderedLoopInvariant = true;

            DateTime ts = iter.next().getTimestamp();
            while (iter.hasNext()) {
                final DateTime nextTs = iter.next().getTimestamp();

                if (ts.equals(nextTs))
                    uniqueLoopInvariant = false;
                if (nextTs.isBefore(ts))
                    orderedLoopInvariant = false;
            }

            ordered = orderedLoopInvariant;
            unique = uniqueLoopInvariant;
        }
    }

    public static WriteableTSDataFile open(Path file) throws IOException {
        final GCCloseable<FileChannel> fd = new GCCloseable<>(FileChannel.open(file, READ, WRITE));
        return new WriteableTSDataFile(fd);
    }

    public static WriteableTSDataFile newFile(Path file, DateTime begin, DateTime end) throws IOException {
        XdrBufferEncodingStream xdr = new XdrBufferEncodingStream();
        tsfile_header header = new tsfile_header();
        header.first = ToXdr.timestamp(begin);
        header.last = ToXdr.timestamp(end);
        try {
            xdr.beginEncoding();
            writeMimeHeader(xdr);
            header.xdrEncode(xdr);
            xdr.endEncoding();
        } catch (OncRpcException ex) {
            throw new RuntimeException(ex);
        }

        final GCCloseable<FileChannel> fd = new GCCloseable<>(FileChannel.open(file, READ, WRITE, CREATE_NEW));
        try {
            write_buffers_(fd.get(), xdr.getBuffers(), null, null, () -> {}, () -> {});
            return new WriteableTSDataFile(fd);
        } catch (IOException | RuntimeException ex) {
            Files.delete(file);
            throw ex;
        }
    }

    private synchronized TSData get_readonly_() {
        TSData result = readonly_.get();
        if (result == null) {
            try {
                result = new UnmappedReadonlyTSDataFile(fd_, ordered, unique);
            } catch (IOException ex) {
                Logger.getLogger(WriteableTSDataFile.class.getName()).log(Level.SEVERE, "read-only stream failed", ex);
                throw new RuntimeException("read-only stream failed", ex);
            }
            readonly_ = new SoftReference<>(result);
        }
        return result;
    }

    @Override
    public boolean isGzipped() { return false; }
    @Override
    public Iterator<TimeSeriesCollection> iterator() { return get_readonly_().iterator(); }
    @Override
    public Spliterator<TimeSeriesCollection> spliterator() { return get_readonly_().spliterator(); }
    @Override
    public Stream<TimeSeriesCollection> stream() { return get_readonly_().stream(); }
    @Override
    public Stream<TimeSeriesCollection> streamReversed() { return get_readonly_().streamReversed(); }
    @Override
    public boolean isEmpty() { return get_readonly_().isEmpty(); }
    @Override
    public int size() { return get_readonly_().size(); }
    @Override
    public boolean contains(Object o) { return get_readonly_().contains(o); }
    @Override
    public Object[] toArray() {
        return get_readonly_().toArray();
    }
    @Override
    public <T> T[] toArray(T[] a) {
        return get_readonly_().toArray(a);
    }

    private static boolean write_buffers_(FileChannel fd, List<ByteBuffer> bufs, HeaderRegenerator header_regenerator, DateTime new_end, Runnable onUnordered, Runnable onNotUnique) throws IOException {
        boolean wrote_something = false;

        final long rollback;
        try {
            rollback = fd.size();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        try {
            long w_offset = rollback;
            for (ByteBuffer buf : bufs) {
                while (buf.hasRemaining()) {
                    long written = fd.write(buf, w_offset);
                    if (written > 0) w_offset += written;

                    if (written != 0) wrote_something = true;
                }
            }

            if (header_regenerator != null)
                header_regenerator.updateTimestamp(requireNonNull(new_end), onUnordered, onNotUnique);
        } catch (RuntimeException | IOException ex) {
            try {
                fd.truncate(rollback);
            } catch (IOException | RuntimeException ex1) {
                Logger.getLogger(WriteableTSDataFile.class.getName()).log(Level.SEVERE, "unable to rollback write", ex1);
            }
            throw ex;
        }

        return wrote_something;
    }

    private synchronized boolean write_buffers_(List<ByteBuffer> bufs, DateTime new_end) {
        try {
            boolean wrote_something = write_buffers_(fd_.get(), bufs, header_regenerator_, new_end, () -> ordered = false, () -> unique = false);
            if (wrote_something)
                readonly_.clear();
            return wrote_something;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean add(TimeSeriesCollection e) {
        if (e.isEmpty()) return false;

        XdrBufferEncodingStream xdr = new XdrBufferEncodingStream();
        final ToXdr to_xdr = new ToXdr(from_xdr_);
        try {
            xdr.beginEncoding();
            to_xdr.data(e).xdrEncode(xdr);
            xdr.endEncoding();
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException(ex);
        }

        return write_buffers_(xdr.getBuffers(), e.getTimestamp());
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return get_readonly_().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesCollection> c) {
        if (c.isEmpty()) return false;

        final XdrBufferEncodingStream xdr = new XdrBufferEncodingStream();
        final ToXdr to_xdr = new ToXdr(from_xdr_);
        try {
            xdr.beginEncoding();
            c.stream()
                    .filter(tsc -> !tsc.isEmpty())
                    .sorted(Comparator.<TimeSeriesCollection, DateTime>comparing(TimeSeriesCollection::getTimestamp))
                    .map(to_xdr::data)
                    .forEach((dp) -> {
                        try {
                            dp.xdrEncode(xdr);
                        } catch (IOException | OncRpcException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
            xdr.endEncoding();
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException(ex);
        }

        DateTime new_end = c.stream().map(TimeSeriesCollection::getTimestamp).max(Comparator.naturalOrder()).get();
        return write_buffers_(xdr.getBuffers(), new_end);
    }

    @Override
    public DateTime getBegin() { return header_regenerator_.getBegin(); }
    @Override
    public DateTime getEnd() { return header_regenerator_.getEnd(); }
    @Override
    public short getMajor() { return header_regenerator_.getMajor(); }
    @Override
    public short getMinor() { return header_regenerator_.getMinor(); }

    @Override
    public long getFileSize() {
        try {
            return fd_.get().size();
        } catch (IOException ex) {
            Logger.getLogger(UnmappedReadonlyTSDataFile.class.getName()).log(Level.SEVERE, "unable to get file size", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.of(fd_);
    }
}
