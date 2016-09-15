package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.history.xdr.MmapReadonlyTSDataFile;
import com.groupon.lex.metrics.history.xdr.UnmappedReadonlyTSDataFile;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import static java.util.Collections.reverse;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.joda.time.DateTime;

/**
 * Models a closeable collection of TimeSeriesValue.
 * @author ariane
 */
public interface TSData extends Collection<TimeSeriesCollection>, CollectHistory {
    public final static int MIN_MMAP_FILESIZE =  0 * 1024 * 1024;
    public final static int MAX_MMAP_FILESIZE = 32 * 1024 * 1024;

    /** Get the lowest timestamp covered by this TSData series. */
    public DateTime getBegin();
    /** Get the highest timestamp covered by this TSData series. */
    public DateTime getEnd();
    /** Retrieve the filesize used by the TSData. */
    public long getFileSize();
    /** Returns the major version of the TSData file.  Note that this is the file-data version, not the application version. */
    public short getMajor();
    /** Returns the minor version of the TSData file.  Note that this is the file-data version, not the application version. */
    public short getMinor();
    /** Returns true if the file is compressed. */
    public boolean isGzipped();
    /** Returns true if the file has ordered records. */
    public boolean isOrdered();
    /** Returns true if the file records are unique. */
    public boolean isUnique();
    /**
     * Returns an iterator for this TSData.
     * Iterator is always ordered, without duplicate timestamps.
     */
    @Override
    public Iterator<TimeSeriesCollection> iterator();

    /**
     * Returns an iterator for this TSData.
     * Iterator is always ordered, without duplicate timestamps.
     */
    @Override
    public default Spliterator<TimeSeriesCollection> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(), NONNULL | IMMUTABLE | ORDERED | DISTINCT | SORTED);
    }

    /**
     * Stream the TSData contents.
     * The stream iterates collection in chronological order, without duplicate timestamps.
     */
    @Override
    public default Stream<TimeSeriesCollection> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Stream the TSData contents in reverse chronological order.
     * The stream iterates collection in reverse chronological order, without duplicate timestamps.
     */
    @Override
    public default Stream<TimeSeriesCollection> streamReversed() {
        final List<TimeSeriesCollection> copy = stream().collect(Collectors.toList());
        reverse(copy);
        return copy.stream();
    }

    /** Test if the TSData is empty. */
    @Override
    public default boolean isEmpty() { return !stream().findAny().isPresent(); }
    /** Returns the number of entries in the TSData. */
    @Override
    public default int size() { return (int)stream().count(); }
    /** Test if the TSData contains the given value. */
    @Override
    public default boolean contains(Object o) { return stream().anyMatch((tsv) -> tsv.equals(o)); }

    /** Test if the TSData contains all the given values. */
    @Override
    public default boolean containsAll(Collection<?> c) {
        Set<?> cc = new HashSet<>(c);  // Make a mutable copy.
        stream().forEach((tsv) -> cc.remove(tsv));
        return cc.isEmpty();
    }

    @Override
    public default Object[] toArray() {
        return stream().toArray();
    }
    @Override
    public default <T> T[] toArray(T[] a) {
        return stream().toArray((size) -> {
            return (T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        });
    }

    @Override
    public boolean add(TimeSeriesCollection tsv);
    @Override
    public default boolean addAll(Collection<? extends TimeSeriesCollection> c) {
        boolean added = false;
        for (TimeSeriesCollection tsv : c) {
            if (add(tsv)) added = true;
        }
        return added;
    }

    /** Unsupported. */
    @Override
    public default void clear() { throw new UnsupportedOperationException("remove"); }
    /** Unsupported. */
    @Override
    public default boolean retainAll(Collection<?> c) { throw new UnsupportedOperationException("remove"); }
    /** Unsupported. */
    @Override
    public default boolean removeAll(Collection<?> c) { throw new UnsupportedOperationException("remove"); }
    /** Unsupported. */
    @Override
    public default boolean remove(Object o) { throw new UnsupportedOperationException("remove"); }
    /** Unsupported. */
    @Override
    public default boolean removeIf(Predicate<? super TimeSeriesCollection> p) { throw new UnsupportedOperationException("remove"); }

    /**
     * Open the given TSData file.
     * @param file The file to open.
     * @return A TSData instance that will read the given file.
     * @throws IOException If an IOException occurs, the file is not a valid TSData file or the version of the file is too new.
     */
    public static TSData readonly(Path file) throws IOException {
        final Logger LOG = Logger.getLogger(TSData.class.getName());
        LOG.log(Level.INFO, "opening {0}", file);

        final long fd_siz = Files.size(file);
        if (fd_siz >= MIN_MMAP_FILESIZE && fd_siz <= MAX_MMAP_FILESIZE) {
            try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
                return new MmapReadonlyTSDataFile(fd.map(FileChannel.MapMode.READ_ONLY, 0, fd_siz));
            }
        } else {
            return UnmappedReadonlyTSDataFile.open(file);
        }
    }

    /** Return the file channel used to read this file, if it is available. */
    public default Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.empty();
    }
}
