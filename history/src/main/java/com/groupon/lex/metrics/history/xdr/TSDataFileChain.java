package com.groupon.lex.metrics.history.xdr;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.list.RWListFile;
import com.groupon.lex.metrics.history.v2.xdr.Util;
import com.groupon.lex.metrics.history.xdr.TSDataScanDir.MetaData;
import com.groupon.lex.metrics.history.xdr.support.FileUtil;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.Closeable;
import java.io.IOException;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.Value;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A writeable TSDataFile, that uses multiple underlying files.
 *
 * @author ariane
 */
public class TSDataFileChain extends SequenceTSData {
    private static final Logger LOG = Logger.getLogger(TSDataFileChain.class.getName());
    public static long MAX_FILESIZE = 512 * 1024 * 1024;
    public static int MAX_FILERECORDS = 17280;  // 1/5th of number of seconds in a day.
    private final long max_filesize_;
    private final int max_filerecords_ = MAX_FILERECORDS;
    private List<Future<?>> pendingTasks = new ArrayList<>();
    private boolean optimizeOldFiles = false;
    private static final LoadingCache<Key, SequenceTSData> FILES = CacheBuilder.newBuilder()
            .concurrencyLevel(Runtime.getRuntime().availableProcessors())
            .weakKeys()
            .softValues()
            .build(new CacheLoader<Key, SequenceTSData>() {
                @Override
                public SequenceTSData load(Key key) throws IOException {
                    return TSData.readonly(key.getFile());
                }
            });
    private final Path dir_;
    private final Set<Key> readKeys = new HashSet<>();
    private Optional<AppendFile> appendFile = Optional.empty();
    private final ReentrantReadWriteLock guard = new ReentrantReadWriteLock(true);  // Protects readKeys and appendFile.

    @NonNull
    @Getter
    @Setter
    private volatile Compression appendCompression = Compression.DEFAULT_APPEND;
    @NonNull
    @Getter
    @Setter
    private volatile Compression optimizedCompression = Compression.DEFAULT_OPTIMIZED;

    private static SequenceTSData getFile(Key key) throws IOException {
        try {
            return FILES.get(key);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof IOException)
                throw (IOException) ex.getCause();
            throw new IllegalStateException("exception not recognized", ex);  // Should never happen.
        }
    }

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

    /**
     * Saved metadata of a single file, used to identify the file and (re)open
     * it.
     */
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

    @RequiredArgsConstructor
    @Getter
    private static class AppendFile {
        private final Path filename;
        private final RWListFile tsdata;
    }

    private TSDataFileChain(@NonNull Path dir, @NonNull Optional<Path> wfile, @NonNull Collection<MetaData> rfiles, long max_filesize) throws IOException {
        dir_ = dir;
        if (wfile.isPresent()) {
            try {
                appendFile = Optional.of(new AppendFile(wfile.get(), new RWListFile(new GCCloseable<>(FileChannel.open(wfile.get(), StandardOpenOption.READ, StandardOpenOption.WRITE)), true)));
            } catch (OncRpcException ex) {
                throw new IOException("decoding failed: " + wfile.get(), ex);
            }
        }
        requireNonNull(rfiles).stream()
                .unordered()
                .map((md) -> new Key(md.getFileName(), md.getBegin(), md.getEnd(), false))
                .forEach(readKeys::add);
        max_filesize_ = max_filesize;
    }

    public static Optional<TSDataFileChain> openDirExisting(@NonNull TSDataScanDir scandir) throws IOException {
        return openDirExisting(scandir, MAX_FILESIZE);
    }

    public static Optional<TSDataFileChain> openDirExisting(@NonNull TSDataScanDir scandir, long max_filesize) throws IOException {
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

    public static TSDataFileChain openDir(@NonNull TSDataScanDir scandir) throws IOException {
        return openDir(scandir, MAX_FILESIZE);
    }

    public static TSDataFileChain openDir(@NonNull TSDataScanDir scandir, long max_filesize) throws IOException {
        TSDataFileChain result = openDirExisting(scandir, max_filesize).orElse(null);
        if (result == null)
            result = new TSDataFileChain(scandir.getDir(), Optional.empty(), emptyList(), max_filesize);
        return result;
    }

    public static Optional<TSDataFileChain> openDirExisting(@NonNull Path dir) throws IOException {
        return openDirExisting(dir, MAX_FILESIZE);
    }

    public static Optional<TSDataFileChain> openDirExisting(@NonNull Path dir, long max_filesize) throws IOException {
        return openDirExisting(new TSDataScanDir(dir), max_filesize);
    }

    public static TSDataFileChain openDir(@NonNull Path dir) throws IOException {
        return openDir(dir, MAX_FILESIZE);
    }

    public static TSDataFileChain openDir(@NonNull Path dir, long max_filesize) throws IOException {
        return openDir(new TSDataScanDir(dir), max_filesize);
    }

    private AppendFile getAppendFileForWriting(DateTime ts_trigger) throws IOException {
        assert guard.isWriteLockedByCurrentThread();

        if (appendFile.filter((fd) -> fd.getTsdata().getFileSize() < max_filesize_ && fd.getTsdata().size() < max_filerecords_).isPresent())
            return appendFile.get();

        final String prefix = String.format("monsoon-%04d%02d%02d-%02d%02d", ts_trigger.getYear(), ts_trigger.getMonthOfYear(), ts_trigger.getDayOfMonth(), ts_trigger.getHourOfDay(), ts_trigger.getMinuteOfHour());
        try {
            final FileUtil.NamedFileChannel newFile = FileUtil.createNewFile(dir_, prefix, ".tsd");
            try {
                final RWListFile fd = RWListFile.newFile(new GCCloseable<>(newFile.getFileChannel()), appendCompression);
                final AppendFile newAppendFile = new AppendFile(newFile.getFileName(), fd);
                installAppendFile(newAppendFile);
                return newAppendFile;
            } catch (Error | RuntimeException | IOException ex) {
                try {
                    Files.delete(newFile.getFileName());
                } catch (Error | RuntimeException | IOException ex1) {
                    ex.addSuppressed(ex1);
                }
                throw ex;
            }
        } catch (IOException ex) {
            if (appendFile.filter((fd) -> fd.getTsdata().getFileSize() < 2 * max_filesize_).isPresent())
                return appendFile.get();
            throw ex;
        }
    }

    @Override
    public ObjectSequence<TimeSeriesCollection> getSequence() {
        return Util.mergeSequences(getRawCollections().stream()
                .map(SequenceTSData::getSequence)
                .toArray(ObjectSequence[]::new));
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
    public long getFileSize() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            return appendFile.map(fd -> fd.getTsdata().getFileSize()).orElse(0L)
                    + readKeys.stream()
                    .mapToLong(key -> {
                        final TSData tsdata = FILES.getIfPresent(key);
                        try {
                            if (tsdata != null)
                                return tsdata.getFileSize();
                            else
                                return Files.size(key.getFile());
                        } catch (IOException ex) {
                            LOG.log(Level.WARNING, "unable to stat file " + key.getFile(), ex);
                            return 0L;
                        }
                    })
                    .sum();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public DateTime getBegin() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            return Stream.concat(appendFile.map(fd -> fd.getTsdata().getBegin()).map(Stream::of).orElseGet(Stream::empty), readKeys.stream().map(Key::getBegin))
                    .min(Comparator.naturalOrder())
                    .orElseGet(() -> new DateTime(0, DateTimeZone.UTC));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public DateTime getEnd() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            return Stream.concat(appendFile.map(fd -> fd.getTsdata().getEnd()).map(Stream::of).orElseGet(Stream::empty), readKeys.stream().map(Key::getEnd))
                    .max(Comparator.naturalOrder())
                    .orElseGet(() -> new DateTime(0, DateTimeZone.UTC));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public short getMajor() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            return appendFile.map(AppendFile::getTsdata).map(TSData::getMajor).orElse(Const.MAJOR);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public short getMinor() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            return appendFile.map(AppendFile::getTsdata).map(TSData::getMinor).orElse(Const.MINOR);
        } finally {
            lock.unlock();
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
                    if (tsdata == null) {
                        try {
                            tsdata = getFile(filesEntry.getKey());
                        } catch (IOException ex) {
                            LOG.log(Level.INFO, "skipping optimization of " + filesEntry.getKey().getFile(), ex);
                            tsdata = null;
                        }
                    }
                    return Optional.ofNullable(tsdata) // Null value if the file was removed between request for compression and now.
                            .map(tsd -> SimpleMapEntry.create(filesEntry.getKey(), tsd));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (files.isEmpty())
            return;
        final List<Key> keys = new ArrayList<>(files.keySet());  // Keys for removal later; we don't want to reference the complete map, as it also holds each TSData and holding on would prevent the GC from retiring them early.
        LOG.log(Level.INFO, "kicking off optimization of {0}", keys.stream().map(Key::getFile).collect(Collectors.toList()));

        optimize(files.values(), new ArrayList<>(files.keySet()));
    }

    private CompletableFuture<Void> optimize(Collection<TSData> tsdata, Collection<Key> erase) {
        final CompletableFuture<Void> task = new TSDataOptimizerTask(dir_, tsdata)
                .withCompression(appendCompression)
                .run()
                .thenAccept((newFile) -> {
                    final ReentrantReadWriteLock.WriteLock lock = guard.writeLock();
                    lock.lock();
                    try {
                        erase.forEach(file -> {
                            if (readKeys.remove(file)) {
                                try {
                                    Files.delete(file.getFile());
                                } catch (IOException ex) {
                                    LOG.log(Level.WARNING, "unable to remove {0}", file.getFile());
                                }
                            }
                        });

                        // Install new key.
                        final Key newKey = new Key(newFile.getName(), newFile.getData().getBegin(), newFile.getData().getEnd(), true);
                        FILES.put(newKey, newFile.getData());
                        readKeys.add(newKey);
                    } finally {
                        lock.unlock();
                    }
                });
        pendingTasks.add(task);
        // Remove self after completion.
        task
                .whenComplete((result, exc) -> {
                    if (exc != null)
                        LOG.log(Level.WARNING, "unable to optimize " + erase.stream().collect(Collectors.toList()), exc);
                    synchronized (this) {
                        pendingTasks.remove(task);
                    }
                });
        return task;
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
            final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
            lock.lock();
            try {
                keys = readKeys.stream()
                        .filter(key -> {
                            try {
                                return !key.isNewFile() && !getFile(key).isOptimized();
                            } catch (IOException ex) {
                                return false;  // Ignore files that fail to open.
                            }
                        })
                        .sorted()
                        .collect(Collectors.toList());
            } finally {
                lock.unlock();
            }

            int count = 0;
            Map<Key, TSData> batch = new HashMap<>();
            for (Key key : keys) {
                if (key.isNewFile())
                    continue;  // New files are handled by file rotation logic.
                final TSData value;
                try {
                    value = getFile(key);
                } catch (IOException ex) {
                    continue;  // Ignore files that fail to open.
                }
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

    private void installAppendFile(@NonNull AppendFile newAppendFile) {
        assert guard.isWriteLockedByCurrentThread();

        if (appendFile.isPresent()) {
            final AppendFile newReadOnlyFile = appendFile.get();
            final Key newKey = new Key(newReadOnlyFile.getFilename(), newReadOnlyFile.getTsdata().getBegin(), newReadOnlyFile.getTsdata().getEnd(), true);
            FILES.put(newKey, newReadOnlyFile.getTsdata());
            readKeys.add(newKey);

            optimizeFiles(singletonMap(newKey, newReadOnlyFile.getTsdata()));
        }

        LOG.log(Level.INFO, "rotating into new file {0}", newAppendFile.getFilename());
        appendFile = Optional.of(newAppendFile);
    }

    @Override
    public boolean add(@NonNull TimeSeriesCollection e) {
        final ReentrantReadWriteLock.WriteLock lock = guard.writeLock();
        lock.lock();
        try {
            return getAppendFileForWriting(e.getTimestamp()).getTsdata().add(e);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean addAll(@NonNull Collection<? extends TimeSeriesCollection> e) {
        if (e.isEmpty())
            return false;

        final ReentrantReadWriteLock.WriteLock lock = guard.writeLock();
        lock.lock();
        try {
            DateTime ts = e.iterator().next().getTimestamp();
            return getAppendFileForWriting(ts).getTsdata().addAll(e);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves keys (file metadata) for all files in this chain.
     */
    public Set<Key> getKeys() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(readKeys));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the file associated with the given key.
     */
    public void delete(@NonNull Key key) {
        final ReentrantReadWriteLock.WriteLock lock = guard.writeLock();
        lock.lock();
        try {
            if (!readKeys.remove(key))
                throw new IllegalArgumentException("key does not exist");
            Files.delete(key.getFile());
        } catch (IOException ex) {
            LOG.log(Level.WARNING, "unable to remove file " + key.getFile(), ex);
        } finally {
            lock.unlock();
        }
    }

    public Collection<SequenceTSData> getRawCollections() {
        final ReentrantReadWriteLock.ReadLock lock = guard.readLock();
        lock.lock();
        try {
            Stream<SequenceTSData> readSequences = readKeys.stream()
                    .flatMap(key -> {
                        try {
                            return Stream.of(getFile(key));
                        } catch (IOException ex) {
                            LOG.log(Level.WARNING, "unable to open {0}, omitting from result", key.getFile());
                            return Stream.empty();
                        }
                    });
            Stream<RWListFile> appendSequences = appendFile
                    .map(AppendFile::getTsdata)
                    .map(Stream::of)
                    .orElseGet(Stream::empty);

            return Stream.concat(readSequences, appendSequences)
                    .parallel()
                    .unordered()
                    .collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.empty();
    }

    public BatchAdd batchAdd() {
        return new BatchAdd();
    }

    public class BatchAdd implements Closeable {
        private final List<TSData> tsdList = new ArrayList<>();
        private long tsdBytes = 0;
        private int tsdRecords = 0;
        private final Collection<CompletableFuture<Void>> outstanding = new ArrayList<>();

        public void add(Collection<? extends TimeSeriesCollection> tsd) {
            if (tsd instanceof TSData)
                add((TSData) tsd);
            else
                TSDataFileChain.this.addAll(tsd);
        }

        public void add(TSData tsd) {
            tsdBytes += tsd.getFileSize();
            tsdRecords += tsd.size();
            tsdList.add(tsd);

            if (tsdBytes >= max_filesize_ || tsdRecords >= max_filerecords_) {
                outstanding.add(optimize(tsdList, emptyList()));
                tsdList.clear();
                tsdBytes = 0;
                tsdRecords = 0;
            }
        }

        public void awaitCompletion() throws IOException {
            try {
                CompletableFuture.allOf(outstanding.toArray(new CompletableFuture[]{}))
                        .get();
            } catch (InterruptedException ex) {
                /* SKIP */
            } catch (ExecutionException ex) {
                // Unpack the cause of the execution exception.
                try {
                    throw ex.getCause();
                } catch (Error | RuntimeException | IOException ex1) {
                    throw ex1;
                } catch (Throwable ex1) {
                    throw new IOException("unable to complete optimization", ex1);
                }
            }
        }

        @Override
        public void close() throws IOException {
            tsdList.forEach(TSDataFileChain.this::addAll);
        }
    }
}
