package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.list.RWListFile;
import com.groupon.lex.metrics.history.xdr.TSDataScanDir.MetaData;
import com.groupon.lex.metrics.history.xdr.support.FileUtil;
import com.groupon.lex.metrics.history.xdr.support.MultiFileIterator;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.history.xdr.support.TSDataMap;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
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
import static java.util.Collections.singletonMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
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
    private boolean optimizeOldFiles = false;

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

    @Value
    public static class Key implements Comparable<Key> {
        @NonNull
        private final Path file;
        @NonNull
        private final DateTime begin;
        @NonNull
        private final DateTime end;
        /**
         * Indicates the file was created/modified since TSDataFileChain
         * started. Note that the write file is always considered new.
         */
        private final boolean newFile;

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
                .map((md) -> new Key(md.getFileName(), md.getBegin(), md.getEnd(), false))
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
     * @param files The keys and associated tsdata for the files to be
     * compressed. Map values may be null.
     */
    private void optimizeFiles(Map<Key, TSData> files) {
        // Fill in all null values in 'files' and remove keys that have been retired.
        files = files.entrySet().stream()
                .map(filesEntry -> {
                    TSData tsdata = filesEntry.getValue();
                    if (tsdata == null)
                        tsdata = read_stores_.get(filesEntry.getKey());
                    return Optional.ofNullable(tsdata) // Null value if the file was removed between request for compression and now.
                            .map(tsd -> SimpleMapEntry.create(filesEntry.getKey(), tsd));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final List<Key> keys = new ArrayList<>(files.keySet());  // Keys for removal later; we don't want to reference the complete map, as it also holds each TSData and holding on would prevent the GC from retiring them early.
        LOG.log(Level.INFO, "kicking off optimization of {0}", keys.stream().map(Key::getFile).collect(Collectors.toList()));

        CompletableFuture<Void> task = new TSDataOptimizerTask(dir_)
                .addAll(files.values())
                .run()
                .thenAccept((newFile) -> {
                    synchronized (this) {
                        keys.forEach(file -> {
                            try {
                                Files.delete(file.getFile());
                            } catch (IOException ex) {
                                LOG.log(Level.WARNING, "unable to remove {0}", file.getFile());
                            }
                            read_stores_.remove(file);
                        });
                        read_stores_.put(new Key(newFile.getName(), newFile.getData().getBegin(), newFile.getData().getEnd(), true), newFile.getData());
                    }
                });
        pendingTasks.add(task);
        // Remove self after completion.
        task
                .whenComplete((result, exc) -> {
                    if (exc != null)
                        LOG.log(Level.WARNING, "unable to optimize " + keys.stream().map(Key::getFile).collect(Collectors.toList()), exc);
                    synchronized (this) {
                        pendingTasks.remove(task);
                    }
                });
    }

    /**
     * Start optimization of old files.
     *
     * @return a completable future of the background selection process. This
     * future can be used to wait for the background selecting thread to
     * complete, or to cancel the operation.
     */
    public CompletableFuture<?> optimizeOldFiles() {
        synchronized (this) {
            if (optimizeOldFiles)
                return CompletableFuture.completedFuture(null);  // Already called.

            CompletableFuture<?> task = new CompletableFuture<>();
            Thread thr = new Thread(() -> optimizeOldFilesTask(task));
            thr.setDaemon(true);
            thr.start();
            pendingTasks.add(task);
            optimizeOldFiles = true;

            // Make the task remove itself after completion.
            task.whenComplete((obj, err) -> {
                try {
                    if (err != null)
                        LOG.log(Level.WARNING, "failed selecting old files for optimization", err);
                } finally {
                    pendingTasks.remove(task);
                }
            });

            return task;
        }
    }

    /**
     * Task that executes gathering data for background collection.
     *
     * @param handle a CompletableFuture that will be completed when the method
     * ends.
     */
    private void optimizeOldFilesTask(CompletableFuture<?> handle) {
        try {
            final List<Key> keys;
            synchronized (this) {
                keys = read_stores_.keySet().stream()
                        .filter(key -> !key.isNewFile() && !read_stores_.get(key).isOptimized())
                        .sorted()
                        .collect(Collectors.toList());
            }

            int count = 0;
            Map<Key, TSData> batch = new HashMap<>();
            for (Key key : keys) {
                if (key.isNewFile())
                    continue;  // New files are handled by file rotation logic.
                final TSData value = read_stores_.get(key);
                if (value == null)
                    continue;  // Key lost.
                if (value.isOptimized())
                    continue;  // Skip already optimized files.

                if (handle.isCancelled())
                    return;
                final int value_size = value.size();  // Takes 2-4 seconds for v0.x and v1.x files!
                count += value_size;
                batch.put(key, value);
                LOG.log(Level.INFO, "added {0} ({1} scrapes) to batch ({2} files, {3} scrapes)", new Object[]{key.getFile(), value_size, batch.size(), count});

                if (count > 50000) {
                    optimizeFiles(batch);
                    // Reset counters.
                    batch.clear();
                    count = 0;
                }
            }
            if (!batch.isEmpty())
                optimizeFiles(batch);
        } catch (Error | Exception ex) {
            handle.completeExceptionally(ex);
        } finally {
            handle.complete(null);
        }
    }

    private synchronized RWListFile install_new_store_(Optional<RWListFile> opt_old_store, Path file, RWListFile new_store) {
        requireNonNull(file);
        requireNonNull(new_store);

        write_filename_.ifPresent(filename -> {
            SequenceTSData old_store = opt_old_store
                    .map(rwfile -> {
                        SequenceTSData tsdata = rwfile;  // Implicit cast.
                        return tsdata;
                    })
                    .orElseGet(() -> {
                        try {
                            return TSData.readonly(filename);
                        } catch (IOException ex) {
                            throw new IllegalStateException("unable to open old write file " + filename, ex);
                        }
                    });
            final Key old_store_key = new Key(filename, old_store.getBegin(), old_store.getEnd(), true);
            read_stores_.put(old_store_key, old_store);

            // Optimize old file.
            optimizeFiles(singletonMap(old_store_key, old_store));
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
        try {
            return get_write_store_for_writing_(e.getTimestamp()).add(e);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesCollection> e) {
        return e.stream()
                .map(TimeSeriesCollection::getTimestamp)
                .min(Comparator.naturalOrder())
                .map((ts) -> {
                    synchronized (this) {
                        try {
                            return get_write_store_for_writing_(ts).addAll(e);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
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

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.empty();
    }
}
