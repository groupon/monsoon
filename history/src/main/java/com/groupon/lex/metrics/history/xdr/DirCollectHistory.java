/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.AbstractCollectHistory;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import static java.util.Objects.requireNonNull;
import java.util.Optional;

/**
 *
 * @author ariane
 */
public class DirCollectHistory extends AbstractCollectHistory<TSDataFileChain> {
    private Optional<Long> disk_usage_limit_ = Optional.empty();

    private static TSDataFileChain scan_dir_(Path dir, Optional<Long> max_filesize) throws IOException {
        return TSDataFileChain.openDir(dir, max_filesize.orElse(TSDataFileChain.MAX_FILESIZE));
    }

    public DirCollectHistory(Path dir, Optional<Long> disk_usage_limit, Optional<Long> max_filesize) throws IOException {
        super(scan_dir_(requireNonNull(dir), max_filesize));
        disk_usage_limit_ = requireNonNull(disk_usage_limit);
    }

    public DirCollectHistory(Path dir) throws IOException {
        this(dir, Optional.empty(), Optional.empty());
    }

    public DirCollectHistory(Path dir, long disk_usage_limit) throws IOException {
        this(dir, Optional.of(disk_usage_limit), Optional.empty());
    }

    public DirCollectHistory(Path dir, long disk_usage_limit, long max_filesize) throws IOException {
        this(dir, Optional.of(disk_usage_limit), Optional.of(max_filesize));
    }

    public boolean hasPendingTasks() {
        return getTSData().hasPendingTasks();
    }

    public void waitPendingTasks() {
        getTSData().waitPendingTasks();
    }

    protected Optional<TSDataFileChain.Key> selectOldestKey() {
        return getTSData().getKeys().stream()
                .sorted(Comparator.comparing(TSDataFileChain.Key::getEnd))
                .findFirst();
    }

    protected void removeKey(TSDataFileChain.Key key) {
        getTSData().delete(key);
    }

    protected void ensureDiskUsageBelow(long bytes) {
        while (getFileSize() > bytes) {
            Optional<TSDataFileChain.Key> to_erase = selectOldestKey();
            if (!to_erase.isPresent()) return;

            removeKey(to_erase.get());
        }
    }

    @Override
    public boolean add(TimeSeriesCollection tsv) {
        final Optional<Long> disk_usage_limit;
        synchronized(this) {
            disk_usage_limit = disk_usage_limit_;
        }

        boolean added = super.add(tsv);
        if (added) disk_usage_limit.ifPresent(this::ensureDiskUsageBelow);
        return added;
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesCollection> tsv) {
        final Optional<Long> disk_usage_limit;
        synchronized(this) {
            disk_usage_limit = disk_usage_limit_;
        }

        boolean added = super.addAll(tsv);
        if (added) disk_usage_limit.ifPresent(this::ensureDiskUsageBelow);
        return added;
    }
}
