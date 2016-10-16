package com.groupon.lex.metrics.history.xdr;

import com.google.common.collect.Iterators;
import static com.groupon.lex.metrics.history.xdr.Const.version_major;
import static com.groupon.lex.metrics.history.xdr.Const.version_minor;
import com.groupon.lex.metrics.history.xdr.support.GzipDecodingBufferSupplier;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID1_EXPECT;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID2_EXPECT;
import com.groupon.lex.metrics.history.xdr.support.Parser;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import com.groupon.lex.metrics.history.xdr.support.XdrStreamIterator;
import com.groupon.lex.metrics.lib.ForwardIterator;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.LazyEval;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.lib.sequence.ReverseSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static java.util.Collections.emptyIterator;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public final class UnmappedReadonlyTSDataFile extends SequenceTSData {
    private static final Logger LOG = Logger.getLogger(UnmappedReadonlyTSDataFile.class.getName());
    private final GCCloseable<FileChannel> fd_;
    private final DateTime begin_, end_;
    private final int version_;
    private final boolean is_gzipped_;
    @Getter
    private final ObjectSequence<TimeSeriesCollection> sequence;

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
        LOG.log(Level.FINE, "instantiated: version={0}.{1} begin={2}, end={3}", new Object[]{version_major(version_), version_minor(version_), begin_, end_});

        sequence = new ForwardIteratingSequence(this::makeIterator)
                .share();
    }

    public static UnmappedReadonlyTSDataFile open(Path file) throws IOException {
        final GCCloseable<FileChannel> fd = new GCCloseable<>(FileChannel.open(file, StandardOpenOption.READ));
        return new UnmappedReadonlyTSDataFile(fd);
    }

    @Override
    public DateTime getBegin() {
        return begin_;
    }

    @Override
    public DateTime getEnd() {
        return end_;
    }

    @Override
    public short getMajor() {
        return version_major(version_);
    }

    @Override
    public short getMinor() {
        return version_minor(version_);
    }

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
    public boolean canAddSingleRecord() {
        return !is_gzipped_;
    }

    @Override
    public boolean isOptimized() {
        return false;
    }

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

    private Iterator<TimeSeriesCollection> makeIterator() {
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

    @RequiredArgsConstructor
    private static class ForwardIteratingSequence implements ObjectSequence<TimeSeriesCollection> {
        private SoftReference<ForwardIterator<TimeSeriesCollection>> iteratorRef = new SoftReference<>(null);
        private final Supplier<Iterator<TimeSeriesCollection>> iteratorSupplier;
        private final LazyEval<Integer> sizeEval = new LazyEval<>(() -> Iterators.size(makeIterator()));
        private final LazyEval<Boolean> emptyEval = new LazyEval<>(() -> !makeIterator().hasNext());

        private synchronized Iterator<TimeSeriesCollection> makeIterator() {
            ForwardIterator<TimeSeriesCollection> iter = iteratorRef.get();
            if (iter == null) {
                iter = new ForwardIterator<>(iteratorSupplier.get());
                iteratorRef = new SoftReference<>(iter);
            }
            return iter.clone();
        }

        @Override
        public boolean isSorted() {
            return true;
        }

        @Override
        public boolean isNonnull() {
            return true;
        }

        @Override
        public boolean isDistinct() {
            return true;
        }

        @Override
        public TimeSeriesCollection get(int index) throws NoSuchElementException {
            if (index < 0)
                throw new NoSuchElementException("index cannot be negative");
            // Only validate index < size up front, if size is already computed.
            // Otherwise, rely on the Iterator to throw NoSuchElementException.
            Optional<Integer> size = sizeEval.getIfPresent();
            if (size.isPresent() && index >= size.get())
                throw new NoSuchElementException(index + " out of range [0.." + size + ")");

            Iterator<TimeSeriesCollection> iter = makeIterator();
            for (int i = 0; i < index; ++i)
                iter.next();
            return iter.next();
        }

        @Override
        public <C extends Comparable<? super C>> Comparator<C> getComparator() {
            return Comparator.naturalOrder();
        }

        @Override
        public Iterator<TimeSeriesCollection> iterator() {
            return makeIterator();
        }

        @Override
        public Spliterator<TimeSeriesCollection> spliterator() {
            return new SpliteratorImpl();
        }

        @Override
        public Stream<TimeSeriesCollection> stream() {
            return StreamSupport.stream(spliterator(), false);
        }

        @Override
        public Stream<TimeSeriesCollection> parallelStream() {
            return StreamSupport.stream(spliterator(), true);
        }

        @Override
        public int size() {
            return sizeEval.get();
        }

        @Override
        public boolean isEmpty() {
            return emptyEval.get();
        }

        @Override
        public ObjectSequence<TimeSeriesCollection> reverse() {
            return new ReverseSequence(0, size())
                    .map(this::get, true, true, true);
        }

        private class SpliteratorImpl implements Spliterator<TimeSeriesCollection> {
            private final Iterator<TimeSeriesCollection> iterator = makeIterator();

            @Override
            public boolean tryAdvance(Consumer<? super TimeSeriesCollection> action) {
                if (!iterator.hasNext())
                    return false;
                action.accept(iterator.next());
                return true;
            }

            @Override
            public Spliterator<TimeSeriesCollection> trySplit() {
                return null;  // Splitting may require reading a lot of data up front.
            }

            @Override
            public long estimateSize() {
                return size();
            }

            @Override
            public int characteristics() {
                return ForwardSequence.SPLITERATOR_CHARACTERISTICS;
            }

            @Override
            public Comparator<TimeSeriesCollection> getComparator() {
                return null;
            }
        }
    }
}
