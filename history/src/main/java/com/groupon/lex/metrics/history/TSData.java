package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.xdr.ColumnMajorTSData;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.history.xdr.support.TmpFileBasedColumnMajorTSData;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
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
 *
 * @author ariane
 */
public interface TSData extends Collection<TimeSeriesCollection>, CollectHistory {
    /**
     * Get the lowest timestamp covered by this TSData series.
     */
    public DateTime getBegin();

    /**
     * Get the highest timestamp covered by this TSData series.
     */
    public DateTime getEnd();

    /**
     * Retrieve the filesize used by the TSData.
     */
    public long getFileSize();

    /**
     * Get the compression used for append files.
     *
     * @return The compression used for append files.
     */
    public Compression getAppendCompression();

    /**
     * Set the compression used for append files.
     *
     * @param compression The compression used for append files.
     */
    public void setAppendCompression(Compression compression);

    /**
     * Get the compression used for optimized files.
     *
     * @return The compression used for optimized files.
     */
    public Compression getOptimizedCompression();

    /**
     * Set the compression used for optimized files.
     *
     * @param compression The compression used for optimized files.
     */
    public void setOptimizedCompression(Compression compression);

    /**
     * Returns the major version of the TSData file. Note that this is the
     * file-data version, not the application version.
     */
    public short getMajor();

    /**
     * Returns the minor version of the TSData file. Note that this is the
     * file-data version, not the application version.
     */
    public short getMinor();

    /**
     * Returns true if the file is suitable for adding single records at a time.
     */
    public boolean canAddSingleRecord();

    /**
     * Returns true if the file is optimized for access by (GroupPath, Tags,
     * MetricName) tuple.
     */
    public boolean isOptimized();

    /**
     * Returns an iterator for this TSData. Iterator is always ordered, without
     * duplicate timestamps.
     */
    @Override
    public Iterator<TimeSeriesCollection> iterator();

    /**
     * Returns an iterator for this TSData. Iterator is always ordered, without
     * duplicate timestamps.
     */
    @Override
    public default Spliterator<TimeSeriesCollection> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(), NONNULL | IMMUTABLE | ORDERED | DISTINCT | SORTED);
    }

    /**
     * Stream the TSData contents. The stream iterates collection in
     * chronological order, without duplicate timestamps.
     */
    @Override
    public default Stream<TimeSeriesCollection> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Stream the TSData contents in reverse chronological order. The stream
     * iterates collection in reverse chronological order, without duplicate
     * timestamps.
     */
    @Override
    public default Stream<TimeSeriesCollection> streamReversed() {
        final List<TimeSeriesCollection> copy = stream().collect(Collectors.toList());
        reverse(copy);
        return copy.stream();
    }

    /**
     * Test if the TSData is empty.
     */
    @Override
    public boolean isEmpty();

    /**
     * Returns the number of entries in the TSData.
     */
    @Override
    public int size();

    /**
     * Test if the TSData contains the given value.
     */
    @Override
    public default boolean contains(Object o) {
        return stream().anyMatch((tsv) -> tsv.equals(o));
    }

    /**
     * Test if the TSData contains all the given values.
     */
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
            return (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        });
    }

    @Override
    public boolean add(TimeSeriesCollection tsv);

    @Override
    public default boolean addAll(Collection<? extends TimeSeriesCollection> c) {
        boolean added = false;
        for (TimeSeriesCollection tsv : c) {
            if (add(tsv))
                added = true;
        }
        return added;
    }

    /**
     * Unsupported.
     */
    @Override
    public default void clear() {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * Unsupported.
     */
    @Override
    public default boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * Unsupported.
     */
    @Override
    public default boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * Unsupported.
     */
    @Override
    public default boolean remove(Object o) {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * Unsupported.
     */
    @Override
    public default boolean removeIf(Predicate<? super TimeSeriesCollection> p) {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * Open the given TSData file.
     *
     * @param file The file to open.
     * @return A TSData instance that will read the given file.
     * @throws IOException If an IOException occurs, the file is not a valid
     * TSData file or the version of the file is too new.
     */
    public static SequenceTSData readonly(Path file) throws IOException {
        final Logger LOG = Logger.getLogger(TSData.class.getName());

        final SequenceTSData result = TSDataVersionDispatch.open(file);
        LOG.log(Level.INFO, "opened v{0}.{1}: {2} ({3}-{4}, {5} scrapes)", new Object[]{result.getMajor(), result.getMinor(), file, result.getBegin(), result.getEnd(), result.getMajor() >= 3 ? result.size() : "#???"});
        return result;
    }

    /**
     * Return the file channel used to read this file, if it is uses a file
     * descriptor. Returns Optional.empty() for memory mapped files.
     */
    public Optional<GCCloseable<FileChannel>> getFileChannel();

    /**
     * Returns the data in column major format.
     *
     * The TSData instance may perform expensive computation to make this work.
     *
     * @return The data in column major format.
     * @throws java.io.IOException if IO errors prevent the computation from
     * succeeding.
     */
    public default ColumnMajorTSData asColumnMajorTSData() throws IOException {
        return TmpFileBasedColumnMajorTSData.builder()
                .with(this)
                .build();
    }
}
