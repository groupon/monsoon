package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.list.RWListFile;
import com.groupon.lex.metrics.history.v2.tables.ToXdrTables;
import com.groupon.lex.metrics.history.xdr.TSDataScanDir.MetaData;
import com.groupon.lex.metrics.history.xdr.support.FileUtil;
import com.groupon.lex.metrics.history.xdr.support.MultiFileIterator;
import com.groupon.lex.metrics.history.xdr.support.TSDataMap;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A writeable TSDataFile, that uses multiple underlying files.
 *
 * @author ariane
 */
public class TSDataFileChain implements TSData {
    private static final Logger LOG = Logger.getLogger(TSDataFileChain.class.getName());
    public static long MAX_FILESIZE = 512 * 1024 * 1024;
    public static int MAX_FILERECORDS = 7220;
    private final long max_filesize_;
    private final int max_filerecords_ = MAX_FILERECORDS;
    private List<Future<?>> pendingTasks = new ArrayList<>();

    /**
     * Tests if there are any pending tasks to be completed.
     */
    public synchronized boolean hasPendingTasks() {
        Iterator<Future<?>> futIter = pendingTasks.iterator();
        while (futIter.hasNext()) {
            Future<?> fut = futIter.next();
            if (fut.isDone())
                futIter.remove();
        }
        return !pendingTasks.isEmpty();
    }

    public void waitPendingTasks() {
        final List<Future<?>> copy;
        synchronized (this) {
            copy = new ArrayList<>(pendingTasks);
        }
        for (Future<?> fut : copy) {
            try {
                fut.get();
            } catch (InterruptedException ex) {
                LOG.log(Level.WARNING, "interrupted while waiting for pending task", ex);
            } catch (ExecutionException ex) {
                LOG.log(Level.WARNING, "pending task completed exceptionally", ex);
            }
        }

        hasPendingTasks();  // Clears out the completed futures.
    }

    @ToString
    public static class Key implements Comparable<Key> {
        private final Path file_;
        private final DateTime begin_;
        private final DateTime end_;

        public Key(Path file, DateTime begin, DateTime end) {
            file_ = requireNonNull(file);
            begin_ = requireNonNull(begin);
            end_ = requireNonNull(end);
        }

        public Path getFile() {
            return file_;
        }

        public DateTime getBegin() {
            return begin_;
        }

        public DateTime getEnd() {
            return end_;
        }

        @Override
        public int compareTo(Key othr) {
            int cmp = 0;
            if (cmp == 0)
                cmp = getBegin().compareTo(othr.getBegin());
            if (cmp == 0)
                cmp = getEnd().compareTo(othr.getEnd());
            if (cmp == 0)
                cmp = getFile().compareTo(othr.getFile());
            return cmp;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 37 * hash + Objects.hashCode(this.begin_);
            hash = 37 * hash + Objects.hashCode(this.end_);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Key other = (Key) obj;
            if (!Objects.equals(this.file_, other.file_)) {
                return false;
            }
            if (!Objects.equals(this.begin_, other.begin_)) {
                return false;
            }
            if (!Objects.equals(this.end_, other.end_)) {
                return false;
            }
            return true;
        }
    }

    private final TSDataMap<Key> read_stores_ = new TSDataMap<>((Key k) -> {
        try {
            return TSData.readonly(k.getFile());
        } catch (IOException ex) {
            throw new RuntimeException("unable to open file " + k.getFile(), ex);
        }
    });
    private final Path dir_;
    private Optional<Path> write_filename_;
    private SoftReference<RWListFile> write_store_;

    private TSDataFileChain(Path dir, Optional<Path> wfile, Collection<MetaData> rfiles, long max_filesize) {
        dir_ = requireNonNull(dir);
        write_filename_ = requireNonNull(wfile);
        write_store_ = new SoftReference<>(null);
        requireNonNull(rfiles).stream()
                .map((md) -> new Key(md.getFileName(), md.getBegin(), md.getEnd()))
                .forEach((key) -> read_stores_.put(key, null));
        max_filesize_ = max_filesize;
    }

    private TSDataFileChain(Path dir, Path wfile, RWListFile wfd, long max_filesize) {
        dir_ = requireNonNull(dir);
        write_filename_ = Optional.of(wfile);
        write_store_ = new SoftReference<>(wfd);
        max_filesize_ = max_filesize;
    }

    public static Optional<TSDataFileChain> openDirExisting(TSDataScanDir scandir) {
        return openDirExisting(scandir, MAX_FILESIZE);
    }

    public static Optional<TSDataFileChain> openDirExisting(TSDataScanDir scandir, long max_filesize) {
        List<MetaData> files = scandir.getFiles();
        if (files.isEmpty())
            return Optional.empty();

        /* If the most recent file is upgradable, use that to append new records to. */
        Optional<MetaData> last = Optional.of(files.get(files.size() - 1))
                .filter((md) -> md.isUpgradable());
        if (last.isPresent())
            files = files.subList(0, files.size() - 1);
        return Optional.of(new TSDataFileChain(scandir.getDir(), last.map(MetaData::getFileName), files, max_filesize));
    }

    public static TSDataFileChain openDir(TSDataScanDir scandir) {
        return openDir(scandir, MAX_FILESIZE);
    }

    public static TSDataFileChain openDir(TSDataScanDir scandir, long max_filesize) {
        return openDirExisting(scandir)
                .orElseGet(() -> {
                    return new TSDataFileChain(scandir.getDir(), Optional.empty(), emptyList(), max_filesize);
                });
    }

    public static Optional<TSDataFileChain> openDirExisting(Path dir) throws IOException {
        return openDirExisting(dir, MAX_FILESIZE);
    }

    public static Optional<TSDataFileChain> openDirExisting(Path dir, long max_filesize) throws IOException {
        return openDirExisting(new TSDataScanDir(dir), max_filesize);
    }

    public static TSDataFileChain openDir(Path dir) throws IOException {
        return openDir(dir, MAX_FILESIZE);
    }

    public static TSDataFileChain openDir(Path dir, long max_filesize) throws IOException {
        return openDir(new TSDataScanDir(dir), max_filesize);
    }

    private synchronized Optional<RWListFile> get_write_store_() throws IOException {
        if (!write_filename_.isPresent())
            return Optional.empty();

        try {
            RWListFile store = write_store_.get();
            if (store == null) {
                store = new RWListFile(new GCCloseable<>(FileChannel.open(write_filename_.get(), StandardOpenOption.READ, StandardOpenOption.WRITE)), true);
                write_store_ = new SoftReference<>(store);
            }
            return Optional.of(store);
        } catch (OncRpcException ex) {
            throw new IOException("failed to open writeable file " + write_filename_.get(), ex);
        }
    }

    private synchronized RWListFile get_write_store_for_writing_(DateTime ts_trigger) throws IOException {
        Optional<RWListFile> opt_store = get_write_store_();
        if (opt_store.filter((fd) -> fd.getFileSize() < max_filesize_ && fd.size() < max_filerecords_).isPresent())
            return opt_store.get();

        try {
            return new_store_(opt_store, ts_trigger);
        } catch (IOException ex) {
            if (opt_store.filter((fd) -> fd.getFileSize() < 2 * max_filesize_).isPresent())
                return opt_store.get();
            throw ex;
        }
    }

    @AllArgsConstructor
    @Getter
    private static abstract class TSDataSupplier implements MultiFileIterator.TSDataSupplier {
        private final DateTime begin;
        private final DateTime end;
    }

    private Stream<TSData> stream_datafiles_() {
        Stream<TSData> wfileStream;
        try {
            wfileStream = get_write_store_().map(Stream::<TSData>of).orElseGet(Stream::empty);
        } catch (IOException ex) {
            wfileStream = Stream.empty();
        }

        final Stream<TSData> rfileStream = read_stores_.entrySet().stream()
                .map(rfile -> rfile.getValue());

        return Stream.concat(wfileStream, rfileStream);
    }

    private Stream<TSDataSupplier> stream_tsdata_suppliers_(Function<TSData, Iterator<TimeSeriesCollection>> iteratorFn) {
        Stream<TSDataSupplier> wfileStream;
        try {
            wfileStream = get_write_store_().map(Stream::of).orElseGet(Stream::empty)
                    .map(wfile -> new TSDataSupplier(wfile.getBegin(), wfile.getEnd()) {
                        @Override
                        public Iterator<TimeSeriesCollection> getIterator() {
                            return iteratorFn.apply(wfile);
                        }
                    });
        } catch (IOException ex) {
            wfileStream = Stream.empty();
        }

        final Stream<TSDataSupplier> rfileStream = read_stores_.entrySet().stream()
                .map(rfile -> new TSDataSupplier(rfile.getKey().getBegin(), rfile.getKey().getEnd()) {
                    @Override
                    public Iterator<TimeSeriesCollection> getIterator() {
                        return iteratorFn.apply(rfile.getValue());
                    }
                });

        return Stream.concat(wfileStream, rfileStream);
    }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        return new MultiFileIterator(stream_tsdata_suppliers_(TSData::iterator).collect(Collectors.toList()), Comparator.naturalOrder());
    }

    @Override
    public Stream<TimeSeriesCollection> streamReversed() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new MultiFileIterator(stream_tsdata_suppliers_(tsdata -> tsdata.streamReversed().iterator()).collect(Collectors.toList()),
                                Comparator.reverseOrder()),
                        NONNULL | IMMUTABLE | ORDERED | DISTINCT),
                false);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        final List<TSDataSupplier> files = stream_tsdata_suppliers_(TSData::iterator)
                .filter(tsd -> !begin.isAfter(tsd.getEnd()))
                .collect(Collectors.toList());
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new MultiFileIterator(files, Comparator.naturalOrder()),
                        NONNULL | IMMUTABLE | ORDERED | DISTINCT),
                false)
                .filter(tsc -> !tsc.getTimestamp().isBefore(begin));
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        final List<TSDataSupplier> files = stream_tsdata_suppliers_(TSData::iterator)
                .filter(tsd -> !begin.isAfter(tsd.getEnd()) && !end.isBefore(tsd.getBegin()))
                .collect(Collectors.toList());
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        new MultiFileIterator(files, Comparator.naturalOrder()),
                        NONNULL | IMMUTABLE | ORDERED | DISTINCT),
                false)
                .filter(tsc -> !tsc.getTimestamp().isBefore(begin))
                .filter(tsc -> !tsc.getTimestamp().isAfter(end));
    }

    @Override
    public boolean isEmpty() {
        return stream_datafiles_()
                .allMatch(TSData::isEmpty);
    }

    @Override
    public boolean canAddSingleRecord() {
        return true;
    }

    @Override
    public boolean isOptimized() {
        return true;
    }

    @Override
    public int size() {
        return stream_datafiles_()
                .mapToInt(TSData::size)
                .sum();
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof TimeSeriesCollection))
            return false;

        final TimeSeriesCollection tsv = (TimeSeriesCollection) o;
        final DateTime ts = tsv.getTimestamp();
        return stream_datafiles_()
                .filter(tsd -> !ts.isBefore(tsd.getBegin()))
                .filter(tsd -> !ts.isAfter(tsd.getEnd()))
                .anyMatch(tsd -> tsd.contains(tsv));
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        /* Only contains TimeSeriesCollections, so filter out the others. */
        final List<TimeSeriesCollection> tsc_list;
        try {
            tsc_list = c.stream()
                    .map(tsc -> (TimeSeriesCollection) tsc)
                    .collect(Collectors.toList());
        } catch (ClassCastException ex) {
            return false;  // Was not a TimeSeriesCollection.
        }

        boolean matches = stream_datafiles_()
                .allMatch(tsd -> {
                    final DateTime begin = tsd.getBegin();
                    final DateTime end = tsd.getEnd();
                    final List<TimeSeriesCollection> filter = tsc_list.stream()
                            .filter(tsc -> !tsc.getTimestamp().isBefore(begin))
                            .filter(tsc -> !tsc.getTimestamp().isAfter(end))
                            .collect(Collectors.toList());

                    if (!filter.isEmpty()) {
                        tsc_list.removeAll(filter);
                        return tsd.containsAll(filter);
                    } else {
                        return true;
                    }
                });
        return matches && tsc_list.isEmpty();
    }

    @Override
    public long getFileSize() {
        final long wfile_size = write_filename_
                .map(wfname -> {
                    try {
                        return Files.size(wfname);
                    } catch (IOException ex) {
                        LOG.log(Level.WARNING, "unable to stat file " + wfname, ex);
                        return 0L;
                    }
                })
                .orElse(0L);
        return wfile_size + read_stores_.entrySet().stream()
                .mapToLong(entry -> {
                    try {
                        return Files.size(entry.getKey().getFile());
                    } catch (IOException ex) {
                        LOG.log(Level.WARNING, "unable to stat file " + entry.getKey().getFile(), ex);
                        return 0L;
                    }
                })
                .sum();
    }

    @Override
    public DateTime getEnd() {
        try {
            return Stream.concat(
                    get_write_store_().map(Stream::of).orElseGet(Stream::empty)
                    .map(TSData::getEnd),
                    read_stores_.keySet().stream()
                    .map(Key::getEnd)
            )
                    .max(Comparator.naturalOrder())
                    .orElseGet(() -> new DateTime(0, DateTimeZone.UTC));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "read error", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public DateTime getBegin() {
        try {
            return Stream.concat(
                    get_write_store_().map(Stream::of).orElseGet(Stream::empty)
                    .map(TSData::getBegin),
                    read_stores_.keySet().stream()
                    .map(Key::getBegin)
            )
                    .min(Comparator.naturalOrder())
                    .orElseGet(() -> new DateTime(0, DateTimeZone.UTC));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "read error", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public short getMajor() {
        try {
            return get_write_store_().map(TSData::getMajor).orElse(Const.MAJOR);
        } catch (IOException ex) {
            return Const.MAJOR;
        }
    }

    @Override
    public short getMinor() {
        try {
            return get_write_store_().map(TSData::getMinor).orElse(Const.MINOR);
        } catch (IOException ex) {
            return Const.MINOR;
        }
    }

    /**
     * Attempts to compress the file denoted by the key.
     *
     * @param file The key of the file to compress.
     * @param tsdata TSData, if it is opened.
     * @return True if the file was compressed and the key was replaced, false
     * otherwise.
     */
    private void compress_file_(Key file, TSData tsdata) throws IOException {
        if (tsdata == null) {
            try {
                tsdata = TSData.readonly(file.getFile());
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "unable to open " + file.getFile(), ex);
                throw ex;
            }
        }

        try (final ToXdrTables tables = new ToXdrTables()) {
            tables.addAll(tsdata);

            final FileChannel compressed_data;
            final Path new_filename;
            {
                final DateTime begin = tsdata.getBegin();
                final String prefix = String.format("monsoon-%04d%02d%02d-%02d%02d", begin.getYear(), begin.getMonthOfYear(), begin.getDayOfMonth(), begin.getHourOfDay(), begin.getMinuteOfHour());

                FileUtil.NamedFileChannel newFile = FileUtil.createNewFile(dir_, prefix, ".optimized");
                new_filename = newFile.getFileName();
                compressed_data = newFile.getFileChannel();
            }

            try {
                try {
                    tables.write(compressed_data);
                } catch (OncRpcException ex) {
                    throw new IOException("encoding failure", ex);
                }

                synchronized (this) {
                    Files.delete(file.getFile());
                    read_stores_.remove(file);  // Remove if present.
                    read_stores_.put(new Key(new_filename, file.getBegin(), file.getEnd()), null);
                }
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, "unable to write optimized file " + new_filename, ex);
                try {
                    Files.delete(new_filename);
                } catch (IOException ex1) {
                    LOG.log(Level.SEVERE, "unable to remove output file " + new_filename, ex1);
                    ex.addSuppressed(ex1);
                }
                throw ex;
            } finally {
                try {
                    assert (compressed_data != null);
                    compressed_data.close();
                } catch (IOException ex) {
                    LOG.log(Level.WARNING, "unable to close optimized file {0}", new_filename);
                }
            }
        }
    }

    private synchronized RWListFile install_new_store_(Optional<RWListFile> old_store, Path file, RWListFile new_store) {
        requireNonNull(file);
        requireNonNull(new_store);

        write_filename_.ifPresent(filename -> {
            final Key old_store_key = new Key(filename, old_store.get().getBegin(), old_store.get().getEnd());
            read_stores_.put(old_store_key, old_store.orElse(null));

            // Optimize in the background, to not block writing of more data.
            pendingTasks.add(ForkJoinPool.commonPool().submit(() -> {
                final long t0 = System.currentTimeMillis();
                try {
                    compress_file_(old_store_key, old_store.orElse(null));
                    LOG.log(Level.INFO, "optimizing file {0} took {1} msec", new Object[]{filename, System.currentTimeMillis() - t0});
                } catch (IOException | RuntimeException ex) {
                    LOG.log(Level.WARNING, "optimizing file " + filename + " failed after " + (System.currentTimeMillis() - t0) + " msec", ex);
                }
            }));
        });

        write_filename_ = Optional.of(file);
        write_store_ = new SoftReference<>(new_store);
        return new_store;
    }

    private RWListFile new_store_(Optional<RWListFile> old_store, DateTime begin) throws IOException {
        final String prefix = String.format("monsoon-%04d%02d%02d-%02d%02d", begin.getYear(), begin.getMonthOfYear(), begin.getDayOfMonth(), begin.getHourOfDay(), begin.getMinuteOfHour());
        FileUtil.NamedFileChannel newFile = FileUtil.createNewFile(dir_, prefix, ".tsd");
        LOG.log(Level.INFO, "rotating into new file {0}", newFile.getFileName());
        return install_new_store_(old_store, newFile.getFileName(), RWListFile.newFile(new GCCloseable<>(newFile.getFileChannel())));
    }

    @Override
    public synchronized boolean add(TimeSeriesCollection e) {
        hasPendingTasks();
        try {
            return get_write_store_for_writing_(e.getTimestamp()).add(e);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesCollection> e) {
        hasPendingTasks();
        return e.stream()
                .map(TimeSeriesCollection::getTimestamp)
                .min(Comparator.naturalOrder())
                .map((ts) -> {
                    try {
                        return get_write_store_for_writing_(ts).addAll(e);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .orElse(false);
    }

    /**
     * Retrieves keys (file metadata) for all files in this chain.
     */
    public Set<Key> getKeys() {
        return Collections.unmodifiableSet(read_stores_.keySet());
    }

    /**
     * Removes the file associated with the given key.
     */
    public void delete(Key key) {
        if (!read_stores_.containsKey(key))
            throw new IllegalArgumentException("key not present");
        try {
            read_stores_.remove(key);
            Files.delete(key.getFile());
        } catch (IOException ex) {
            LOG.log(Level.WARNING, "unable to remove file " + key.getFile(), ex);
        }
    }
}
