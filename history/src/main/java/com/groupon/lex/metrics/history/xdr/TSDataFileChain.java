package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.xdr.TSDataScanDir.MetaData;
import com.groupon.lex.metrics.history.xdr.support.TSDataMap;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import static java.nio.channels.Channels.newOutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;
import static java.util.Collections.reverse;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import java.util.Spliterators;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A writeable TSDataFile, that uses multiple underlying files.
 * @author ariane
 */
public class TSDataFileChain implements TSData {
    private static final Logger LOG = Logger.getLogger(TSDataFileChain.class.getName());
    public static long MAX_FILESIZE = 64 * 1024 * 1024;
    private final long max_filesize_;

    public static class Key implements Comparable<Key> {
        private final Path file_;
        private final DateTime begin_;
        private final DateTime end_;

        public Key(Path file, DateTime begin, DateTime end) {
            file_ = requireNonNull(file);
            begin_ = requireNonNull(begin);
            end_ = requireNonNull(end);
        }

        public Path getFile() { return file_; }
        public DateTime getBegin() { return begin_; }
        public DateTime getEnd() { return end_; }

        @Override
        public int compareTo(Key othr) {
            int cmp = 0;
            if (cmp == 0) cmp = getBegin().compareTo(othr.getBegin());
            if (cmp == 0) cmp = getEnd().compareTo(othr.getEnd());
            if (cmp == 0) cmp = getFile().compareTo(othr.getFile());
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
    private SoftReference<WriteableTSDataFile> write_store_;

    private TSDataFileChain(Path dir, Optional<Path> wfile, Collection<MetaData> rfiles, long max_filesize) {
        dir_ = requireNonNull(dir);
        write_filename_ = requireNonNull(wfile);
        write_store_ = new SoftReference<>(null);
        requireNonNull(rfiles).stream()
                .map((md) -> new Key(md.getFileName(), md.getBegin(), md.getEnd()))
                .forEach((key) -> read_stores_.put(key, null));
        max_filesize_ = max_filesize;
    }

    private TSDataFileChain(Path dir, Path wfile, WriteableTSDataFile wfd, long max_filesize) {
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
        if (files.isEmpty()) return Optional.empty();

        /* If the most recent file is upgradable, use that to append new records to. */
        Optional<MetaData> last = Optional.of(files.get(files.size() - 1))
                .filter((md) -> md.isUpgradable());
        if (last.isPresent()) files = files.subList(0, files.size() - 1);
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

    private synchronized Optional<WriteableTSDataFile> get_write_store_() throws IOException {
        if (!write_filename_.isPresent()) return Optional.empty();

        WriteableTSDataFile store = write_store_.get();
        if (store == null) {
            store = WriteableTSDataFile.open(write_filename_.get());
            write_store_ = new SoftReference<>(store);
        }
        return Optional.of(store);
    }

    private synchronized WriteableTSDataFile get_write_store_for_writing_(DateTime ts_trigger) throws IOException {
        Optional<WriteableTSDataFile> opt_store = get_write_store_();
        if (opt_store.filter((fd) -> fd.getFileSize() < max_filesize_).isPresent()) return opt_store.get();

        try {
            return new_store_(opt_store, ts_trigger);
        } catch (IOException ex) {
            if (opt_store.filter((fd) -> fd.getFileSize() < 2 * max_filesize_).isPresent()) return opt_store.get();
            throw ex;
        }
    }

    private Stream<Map.Entry<Key, TSData>> stream_datafiles_() {
        Stream<Map.Entry<Key, TSData>> read = read_stores_.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().getBegin()));

        Optional<WriteableTSDataFile> opt_ws;
        try {
            opt_ws = get_write_store_();
        } catch (IOException ex) {
            opt_ws = Optional.empty();
        }
        Stream<Map.Entry<Key, TSData>> write = opt_ws
                .flatMap(ws -> write_filename_.map(name -> SimpleMapEntry.create(name, ws)))
                .map(named_ws -> {
                    final Path name = named_ws.getKey();
                    final WriteableTSDataFile ws = named_ws.getValue();
                    final Key key = new Key(name, ws.getBegin(), ws.getEnd());
                    return SimpleMapEntry.<Key, TSData>create(key, ws);
                })
                .map(Stream::of)
                .orElseGet(Stream::empty);

        return Stream.concat(read, write);
    }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        return stream().iterator();
    }

    @Override
    public Spliterator<TimeSeriesCollection> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(), NONNULL | IMMUTABLE | ORDERED);
    }

    @Override
    public Stream<TimeSeriesCollection> stream() {
        return stream_datafiles_()
                .map(Map.Entry::getValue)
                .flatMap(TSData::stream);
    }

    @Override
    public Stream<TimeSeriesCollection> streamReversed() {
        final List<TSData> reversed = stream_datafiles_()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        reverse(reversed);
        return reversed.stream()
                .flatMap(TSData::streamReversed);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        return stream_datafiles_()
                .filter(tsd -> !begin.isAfter(tsd.getKey().getEnd()))
                .map(Map.Entry::getValue)
                .flatMap(TSData::stream)
                .filter(tsc -> !tsc.getTimestamp().isBefore(begin));
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        return stream_datafiles_()
                .filter(tsd -> !begin.isAfter(tsd.getKey().getEnd()) && !end.isBefore(tsd.getKey().getBegin()))
                .map(Map.Entry::getValue)
                .flatMap(TSData::stream)
                .filter(tsc -> !tsc.getTimestamp().isBefore(begin))
                .filter(tsc -> !tsc.getTimestamp().isAfter(end));
    }

    @Override
    public boolean isEmpty() {
        return stream_datafiles_()
                .map(Map.Entry::getValue)
                .allMatch(TSData::isEmpty);
    }

    @Override
    public boolean isGzipped() {
        return false;
    }

    @Override
    public int size() {
        return stream_datafiles_()
                .map(Map.Entry::getValue)
                .mapToInt(TSData::size)
                .sum();
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof TimeSeriesCollection)) return false;

        final TimeSeriesCollection tsv = (TimeSeriesCollection)o;
        final DateTime ts = tsv.getTimestamp();
        return stream_datafiles_()
                .filter(tsd -> !ts.isBefore(tsd.getKey().getBegin()))
                .filter(tsd -> !ts.isAfter(tsd.getKey().getEnd()))
                .map(Map.Entry::getValue)
                .anyMatch(tsd -> tsd.contains(tsv));
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        /* Only contains TimeSeriesCollections, so filter out the others. */
        final List<TimeSeriesCollection> tsc_list;
        try {
            tsc_list = c.stream()
                    .map(tsc -> (TimeSeriesCollection)tsc)
                    .collect(Collectors.toList());
        } catch (ClassCastException ex) {
            return false;  // Was not a TimeSeriesCollection.
        }

        boolean matches = stream_datafiles_()
                .unordered()
                .allMatch(tsd -> {
                    final DateTime begin = tsd.getKey().getBegin();
                    final DateTime end = tsd.getKey().getEnd();
                    final List<TimeSeriesCollection> filter = tsc_list.stream()
                            .filter(tsc -> !tsc.getTimestamp().isBefore(begin))
                            .filter(tsc -> !tsc.getTimestamp().isAfter(end))
                            .collect(Collectors.toList());

                    if (!filter.isEmpty()) {
                        tsc_list.removeAll(filter);
                        return tsd.getValue().containsAll(filter);
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
     * @param file The key of the file to compress.
     * @param tsdata TSData, if it is opened.
     * @return True if the file was compressed and the key was replaced, false otherwise.
     */
    private synchronized boolean compress_file_(Key file, TSData tsdata) {
        if (tsdata == null) {
            try {
                tsdata = TSData.readonly(file.getFile());
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "unable to open " + file.getFile(), ex);
                return false;
            }
        }

        if (tsdata.isGzipped()) return false;
        final Optional<GCCloseable<FileChannel>> opt_channel = tsdata.getFileChannel();
        final GCCloseable<FileChannel> channel;
        if (opt_channel.isPresent()) {
            channel = opt_channel.get();
        } else {
            try {
                channel = new GCCloseable<>(FileChannel.open(file.getFile(), StandardOpenOption.READ));
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "unable to open file: " + file + ", skipping compression...", ex);
                return false;
            }
        }

        final String base_name = file.getFile().getFileName().toString();
        FileChannel compressed_data;
        Path new_filename;
        for (int i = 0; true; ++i) {
            new_filename = file.getFile().resolveSibling(base_name + (i == 0 ? "" : "." + i) + ".gz");
            try {
                compressed_data = FileChannel.open(new_filename, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                break;
            } catch (IOException ex) {
                LOG.log(Level.INFO, "unable to create new file " + new_filename, ex);
                new_filename = null;
                compressed_data = null;
            }
        }
        if (compressed_data == null) {
            LOG.log(Level.WARNING, "unable to create new file for compressing {0}", file);
            return false;
        }

        try (WritableByteChannel output = Channels.newChannel(new GZIPOutputStream(newOutputStream(compressed_data)))) {
            final FileChannel fd = channel.get();
            final long size = fd.size();

            final ByteBuffer buf = ByteBuffer.allocateDirect(4096);
            long pos = 0;
            for (;;) {
                int rlen;
                rlen = fd.read(buf, pos);
                if (rlen == -1) break;
                pos += rlen;
                buf.flip();

                output.write(buf);
                buf.compact();
            }
            if (pos != size)
                throw new IOException("incorrect number of bytes written");

            Files.delete(file.getFile());
            read_stores_.remove(file);  // Remove if present.
            read_stores_.put(new Key(new_filename, file.getBegin(), file.getEnd()), null);
            return true;
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "unable to write gzip file " + new_filename, ex);
            try {
                Files.delete(new_filename);
            } catch (IOException ex1) {
                LOG.log(Level.SEVERE, "unable to remove output file " + new_filename, ex1);
            }
            return false;
        }
    }

    private synchronized WriteableTSDataFile install_new_store_(Optional<WriteableTSDataFile> old_store, Path file, WriteableTSDataFile new_store) {
        requireNonNull(file);
        requireNonNull(new_store);

        write_filename_.ifPresent(filename -> {
            final Key old_store_key = new Key(filename, old_store.get().getBegin(), old_store.get().getEnd());
            read_stores_.put(old_store_key, old_store.orElse(null));
            compress_file_(old_store_key, old_store.orElse(null));
        });

        write_filename_ = Optional.of(file);
        write_store_ = new SoftReference<>(new_store);
        return new_store;
    }

    private WriteableTSDataFile new_store_(Optional<WriteableTSDataFile> old_store, DateTime begin) throws IOException {
        requireNonNull(begin);
        final String prefix = String.format("monsoon-%04d%02d%02d-%02d%02d", begin.getYear(), begin.getMonthOfYear(), begin.getDayOfMonth(), begin.getHourOfDay(), begin.getMinuteOfHour());

        /*
         * Try with normal basename.
         */
        String name = prefix + ".tsd";
        Path new_filename = dir_.resolve(name);
        try {
            return install_new_store_(old_store, new_filename, WriteableTSDataFile.newFile(new_filename, begin, begin));
        } catch (IOException ex) {
            /* Ignore. */
        }

        /*
         * Try with random number as differentiator between filenames.
         */
        SecureRandom random = new SecureRandom();
        for (int i = 0; i < 15; ++i) {
            long idx = random.nextLong();
            name = String.format("%s-%d.tsd", prefix, idx);
            new_filename = dir_.resolve(name);
            try {
                return install_new_store_(old_store, new_filename, WriteableTSDataFile.newFile(new_filename, begin, begin));
            } catch (IOException ex) {
                /* Ignore. */
            }
        }

        /*
         * Try one more time with differentiator, but let the exception out this time.
         */
        long idx = random.nextLong();
        name = String.format("%s-%d.tsd", prefix, idx);
        new_filename = dir_.resolve(name);
        return install_new_store_(old_store, new_filename, WriteableTSDataFile.newFile(new_filename, begin, begin));
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
                    try {
                        return get_write_store_for_writing_(ts).addAll(e);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .orElse(false);
    }

    /** Retrieves keys (file metadata) for all files in this chain. */
    public Set<Key> getKeys() {
        return Collections.unmodifiableSet(read_stores_.keySet());
    }

    /** Removes the file associated with the given key. */
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
