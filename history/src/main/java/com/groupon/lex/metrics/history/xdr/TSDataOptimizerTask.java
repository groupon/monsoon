package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.TSDataVersionDispatch.Releaseable;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.tables.ReadonlyTableFile;
import com.groupon.lex.metrics.history.v2.tables.ToXdrTables;
import com.groupon.lex.metrics.history.xdr.support.FileUtil;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A task that optimizes a set of files into a single table file.
 *
 * @author ariane
 */
public class TSDataOptimizerTask {
    private static final Logger LOG = Logger.getLogger(TSDataOptimizerTask.class.getName());
    private static final ForkJoinPool TASK_POOL = new ForkJoinPool(Runtime.getRuntime().availableProcessors(), defaultForkJoinWorkerThreadFactory, null, true);
    private static final ExecutorService INSTALL_POOL = Executors.newFixedThreadPool(1);
    private static final List<CompletableFuture<NewFile>> OUTSTANDING = new LinkedList<>();
    private final Path destDir;
    private Map<Path, Reference<TSData>> files = new HashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                OUTSTANDING.forEach(fut -> fut.cancel(false));
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
     * @param files zero or more files to add; note that the value of the map is
     * allowed to be null.
     */
    public TSDataOptimizerTask(@NonNull Path destDir, @NonNull Map<Path, TSData> files) {
        this(destDir);
        files.forEach(this::add);
    }

    /**
     * Create a new optimizer task and fill it with the given files.
     *
     * @param destDir the destination directory in which the new file will be
     * placed.
     * @param files a list of filenames to add.
     */
    public TSDataOptimizerTask(@NonNull Path destDir, @NonNull Collection<Path> files) {
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
     * @param file The name of the file to be added.
     * @param tsdata The TSData obtained when opening this file; may be null.
     * @return this TSDataOptimizerTask.
     */
    public TSDataOptimizerTask add(@NonNull Path file, TSData tsdata) {
        files.put(file, new WeakReference<>(tsdata));
        return this;
    }

    /**
     * Add a file to the collection of files that will make up the final
     * optimized file.
     *
     * @param file The name of the file to be added.
     * @return this TSDataOptimizerTask.
     */
    public TSDataOptimizerTask add(@NonNull Path file) {
        add(file, null);
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
        final Map<Path, Reference<TSData>> jfpFiles = this.files;  // We clear out files below, which makes fjpCreateTmpFile see an empty map if we don't use a separate variable.
        TASK_POOL.submit(() -> fjpCreateTmpFile(fileCreation, destDir, jfpFiles));
        this.files = new HashMap<>();
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
    private static void fjpCreateTmpFile(CompletableFuture<NewFile> fileCreation, Path destDir, Map<Path, Reference<TSData>> files) {
        LOG.log(Level.FINE, "starting temporary file creation...");
        try {
            final FileChannel fd = FileUtil.createTempFile(destDir, "monsoon-", ".optimize-tmp");
            try {
                final DateTime begin;
                try (ToXdrTables output = new ToXdrTables(fd, Compression.DEFAULT_OPTIMIZED)) {
                    for (Map.Entry<Path, Reference<TSData>> entry : files.entrySet()) {
                        if (fileCreation.isCancelled())
                            throw new IOException("aborted due to canceled execution");

                        TSData tsdata = entry.getValue().get();
                        if (tsdata == null)
                            tsdata = TSData.readonly(entry.getKey());
                        output.addAll(tsdata);
                    }
                    begin = new DateTime(output.getHdrBegin(), DateTimeZone.UTC);

                    if (fileCreation.isCancelled())
                        throw new IOException("aborted due to canceled execution");
                }  // Closing output takes a lot of time.

                if (fileCreation.isCancelled())  // Recheck after closing output.
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
                OUTSTANDING.remove(fileCreation);
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
        private final TSData data;
    }
}
