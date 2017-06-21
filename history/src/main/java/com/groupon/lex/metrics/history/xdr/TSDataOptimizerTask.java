package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.TSDataVersionDispatch.Releaseable;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.tables.ReadonlyTableFile;
import com.groupon.lex.metrics.history.v2.tables.ToXdrTables;
import com.groupon.lex.metrics.history.xdr.support.FileUtil;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

/**
 * A task that optimizes a set of files into a single table file.
 *
 * @author ariane
 */
public class TSDataOptimizerTask {
    private static final Logger LOG = Logger.getLogger(TSDataOptimizerTask.class.getName());
    private static final AtomicInteger TASK_POOL_IDX = new AtomicInteger();
    private static final AtomicInteger INSTALL_POOL_IDX = new AtomicInteger();

    /**
     * The task pool handles the creation of temporary files containing all
     * data. The task is highly CPU bound (especially the gathering of data
     * stage in ToXdrTables). Limiting it to 1 thread ensures the tasks don't
     * overwhelm the ForkJoinPool and the limit of 1 thread means multiple
     * compression actions will be queued one-after-the-other.
     *
     * The task itself mainly uses the ForkJoinPool, so this thread spends most
     * of the time waiting for work to complete (or new work to come in).
     */
    private static final ExecutorService TASK_POOL = Executors.newFixedThreadPool(1, (Runnable r) -> {
        Thread thr = new Thread(r);
        thr.setDaemon(true);
        thr.setName("TSDataOptimizerTask-TaskPool-" + TASK_POOL_IDX.incrementAndGet());
        return thr;
    });

    /**
     * The install pool handles the file installation part of creating a new
     * tables file. It is IO bound, simply copying from a temporary file to the
     * final file.
     *
     * Since it's IO bound, it won't interfere with ForkJoinPool tasks. It is a
     * separate thread from the task pool, to allow the task pool to pick up a
     * new file while the old file is being written out.
     */
    private static final ExecutorService INSTALL_POOL = Executors.newFixedThreadPool(1, (Runnable r) -> {
        Thread thr = new Thread(r);
        thr.setDaemon(true);
        thr.setName("TSDataOptimizerTask-InstallPool-" + INSTALL_POOL_IDX.incrementAndGet());
        return thr;
    });

    /**
     * List of outstanding futures. The list is used during program termination
     * to cancel all incomplete futures and thus shut down any dependant tasks
     * properly.
     *
     * Access is {@code synchronized(OUTSTANDING)}.
     */
    private static final List<CompletableFuture<NewFile>> OUTSTANDING = new LinkedList<>();

    /**
     * Destination directory in which to write files. Also used as a location
     * for temporary files.
     */
    private final Path destDir;

    @NonNull
    @Getter
    @Setter
    private Compression compression = Compression.DEFAULT_OPTIMIZED;

    /**
     * List of files to add to the generated tables file.
     */
    private List<TSData> files = new LinkedList<>();

    static {
        // Create shutdown hook that cancels all outstanding futures and tears
        // down the threads.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                synchronized (OUTSTANDING) {
                    OUTSTANDING.forEach(fut -> fut.cancel(false));
                }
                INSTALL_POOL.shutdown();
                TASK_POOL.shutdown();
                if (!INSTALL_POOL.awaitTermination(30, TimeUnit.SECONDS))
                    LOG.log(Level.WARNING, "Install pool did not shut down after 30 seconds.");
            } catch (InterruptedException ex) {
                LOG.log(Level.SEVERE, "Interrupted while waiting for clean shutdown of install pool.", ex);
            }
        }));
    }

    /**
     * Create a new optimizer task and fill it with the given files.
     *
     * @param destDir the destination directory in which the new file will be
     * placed.
     * @param files a list of files to add.
     */
    public TSDataOptimizerTask(@NonNull Path destDir, @NonNull Collection<TSData> files) {
        this(destDir);
        files.forEach(this::add);
    }

    /**
     * Create a new optimizer task and fill it with the given files.
     *
     * @param destDir the destination directory in which the new file will be
     * placed.
     */
    public TSDataOptimizerTask(@NonNull Path destDir) {
        this.destDir = destDir;
        if (!Files.isDirectory(destDir))
            throw new IllegalArgumentException(destDir + " is not a directory");
    }

    /**
     * Add a file to the collection of files that will make up the final
     * optimized file.
     *
     * @param tsdata The TSData of which the contents are to be added to the
     * optimized file.
     * @return this TSDataOptimizerTask.
     */
    public TSDataOptimizerTask add(TSData tsdata) {
        files.add(tsdata);
        return this;
    }

    /**
     * Add files to the collection of files that will make up the final
     * optimized file.
     *
     * @param tsdata The collection of TSData of which the contents are to be
     * added to the optimized file.
     * @return this TSDataOptimizerTask.
     */
    public TSDataOptimizerTask addAll(Collection<? extends TSData> tsdata) {
        tsdata.forEach(this::add);
        return this;
    }

    /**
     * Use the specified compression method for creating the optimized file.
     *
     * @param compression The compression method to use.
     * @return this TSDataOptimizerTask.
     */
    public TSDataOptimizerTask withCompression(Compression compression) {
        setCompression(compression);
        return this;
    }

    /**
     * Start creating the optimized file. This operation resets the state of the
     * optimizer task, so it can be re-used for subsequent invocations.
     *
     * @return A completeable future that yields the newly created file.
     */
    public CompletableFuture<NewFile> run() {
        LOG.log(Level.FINE, "starting optimized file creation for {0} files", files.size());
        CompletableFuture<NewFile> fileCreation = new CompletableFuture<>();
        final List<TSData> fjpFiles = this.files;  // We clear out files below, which makes createTmpFile see an empty map if we don't use a separate variable.
        TASK_POOL.execute(() -> createTmpFile(fileCreation, destDir, fjpFiles, getCompression()));
        synchronized (OUTSTANDING) {
            OUTSTANDING.add(fileCreation);
        }
        this.files = new LinkedList<>();  // Do not use clear! This instance is now shared with the createTmpFile task.
        return fileCreation;
    }

    /**
     * The fork-join task that creates a new file. This function creates a
     * temporary file with the result contents, then passes it on to the install
     * thread which will put the final file in place.
     *
     * @param fileCreation the future that is to be completed after the
     * operation.
     * @param destDir the destination directory for the result; also used for
     * temporary file creation.
     * @param files the list of files that make up the resulting file.
     */
    private static void createTmpFile(CompletableFuture<NewFile> fileCreation, Path destDir, List<TSData> files, Compression compression) {
        LOG.log(Level.FINE, "starting temporary file creation...");

        try {
            Collections.sort(files, Comparator.comparing(TSData::getBegin));

            final FileChannel fd = FileUtil.createTempFile(destDir, "monsoon-", ".optimize-tmp");
            try {
                final DateTime begin;
                try (ToXdrTables output = new ToXdrTables()) {
                    while (!files.isEmpty()) {
                        TSData tsdata = files.remove(0);
                        if (fileCreation.isCancelled())
                            throw new IOException("aborted due to canceled execution");
                        output.addAll(tsdata);  // Takes a long time.
                    }

                    if (fileCreation.isCancelled())
                        throw new IOException("aborted due to canceled execution");
                    begin = output.build(fd, compression); // Writing output takes a lot of time.
                }

                if (fileCreation.isCancelled()) // Recheck after closing output.
                    throw new IOException("aborted due to canceled execution");

                // Forward the temporary file to the installation, which will complete the operation.
                INSTALL_POOL.execute(() -> install(fileCreation, destDir, fd, begin));
            } catch (Error | RuntimeException | IOException ex) {
                try {
                    fd.close();
                } catch (Error | RuntimeException | IOException ex1) {
                    ex.addSuppressed(ex1);
                }
                throw ex;
            }
        } catch (Error | RuntimeException | IOException ex) {
            LOG.log(Level.WARNING, "temporary file for optimization failure", ex);
            synchronized (OUTSTANDING) {
                OUTSTANDING.remove(fileCreation);
            }
            fileCreation.completeExceptionally(ex);  // Propagate exceptions.
        }
    }

    /**
     * Installs the newly created file. This function runs on the install thread
     * and essentially performs a copy-operation from the temporary file to the
     * final file.
     *
     * @param fileCreation the completeable future that receives the newly
     * created file.
     * @param destDir the destination directory in which to install the result
     * file.
     * @param tmpFile the temporary file used to create the data; will be closed
     * by this function.
     * @param begin a timestamp indicating where this file begins; used to
     * generate a pretty file name.
     */
    private static void install(CompletableFuture<NewFile> fileCreation, Path destDir, FileChannel tmpFile, DateTime begin) {
        try {
            try {
                synchronized (OUTSTANDING) {
                    OUTSTANDING.remove(fileCreation);
                }
                if (fileCreation.isCancelled())
                    throw new IOException("Installation aborted, due to cancellation.");

                final FileUtil.NamedFileChannel newFile = FileUtil.createNewFile(destDir, prefixForTimestamp(begin), ".optimized");
                try (Releaseable<FileChannel> out = new Releaseable<>(newFile.getFileChannel())) {
                    final long fileSize = tmpFile.size();
                    LOG.log(Level.INFO, "installing {0} ({1} MB)", new Object[]{newFile.getFileName(), fileSize / 1024.0 / 1024.0});

                    // Copy tmpFile to out.
                    long offset = 0;
                    while (offset < fileSize)
                        offset += tmpFile.transferTo(offset, fileSize - offset, out.get());
                    out.get().force(true);  // Ensure new file is safely written to permanent storage.

                    // Complete future with newly created file.
                    fileCreation.complete(new NewFile(newFile.getFileName(), new ReadonlyTableFile(new GCCloseable<>(out.release()))));
                } catch (Error | RuntimeException | IOException | OncRpcException ex) {
                    // Ensure new file gets destroyed if an error occurs during copying.
                    try {
                        Files.delete(newFile.getFileName());
                    } catch (Error | RuntimeException | IOException ex1) {
                        ex.addSuppressed(ex1);
                    }
                    throw ex;
                }
            } finally {
                // Close tmp file that we got from fjpCreateTmpFile.
                tmpFile.close();
            }
        } catch (Error | RuntimeException | IOException | OncRpcException ex) {
            LOG.log(Level.WARNING, "unable to install new file", ex);
            fileCreation.completeExceptionally(ex);  // Propagate error to future.
        }
    }

    /**
     * Compute a prefix for a to-be-installed file.
     *
     * @param timestamp the timestamp on which to base the prefix.
     * @return a file prefix that represents the timestamp in its name.
     */
    private static String prefixForTimestamp(DateTime timestamp) {
        return String.format("monsoon-%04d%02d%02d-%02d%02d", timestamp.getYear(), timestamp.getMonthOfYear(), timestamp.getDayOfMonth(), timestamp.getHourOfDay(), timestamp.getMinuteOfHour());
    }

    /**
     * The result of installing a new optimized file.
     */
    @RequiredArgsConstructor
    @Getter
    public static class NewFile {
        /**
         * The name of the newly installed file.
         */
        @NonNull
        private final Path name;
        /**
         * The contents of the newly installed file.
         */
        @NonNull
        private final ReadonlyTableFile data;
    }
}
