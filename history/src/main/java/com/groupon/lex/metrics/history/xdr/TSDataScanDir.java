/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;

/**
 * Scan all files in the given directory and retain a list of all files that matched.
 * @author ariane
 */
public class TSDataScanDir {
    /**
     * Metadata for a given TSData file.
     */
    public static class MetaData {
        private final short version_minor_, version_major_;
        private final DateTime begin_, end_;
        private final Path filename_;
        private final long file_size_;
        private final boolean is_gzipped_;

        private MetaData(Path filename, DateTime begin, DateTime end, short version_major, short version_minor, long file_size, boolean is_gzipped) {
            filename_ = requireNonNull(filename);
            begin_ = requireNonNull(begin);
            end_ = requireNonNull(end);
            if (version_major < 0 || version_minor < 0)
                throw new IllegalArgumentException("negative version numbers are not supported");
            version_major_ = version_major;
            version_minor_ = version_minor;
            if (file_size < 0)
                throw new IllegalArgumentException("negative file size is not supported");
            file_size_ = file_size;
            is_gzipped_ = is_gzipped;
        }

        public static Optional<MetaData> fromFile(Path filename) {
            try {
                final TSData fd = TSData.readonly(requireNonNull(filename));
                return Optional.of(new MetaData(filename, fd.getBegin(), fd.getEnd(), fd.getMajor(), fd.getMinor(), fd.getFileSize(), fd.isGzipped()));
            } catch (IOException ex) {
                return Optional.empty();
            }
        }

        /** The name of the file. */
        public Path getFileName() { return filename_; }
        /** The lowest timestamp in the file. */
        public DateTime getBegin() { return begin_; }
        /** The highest timestamp in the file. */
        public DateTime getEnd() { return end_; }
        /** The major version of the file data. */
        public short getVersionMajor() { return version_major_; }
        /** The minor version of the file data. */
        public short getVersionMinor() { return version_minor_; }
        /** The size of the file. */
        public long getFileSize() { return file_size_; }
        /** True if the file is compressed. */
        public boolean isGzipped() { return is_gzipped_; }

        /**
         * Check if the file is upgradable to the latest version
         * of the file data used by the implementation.
         *
         * If a file is upgradable, it means that the data inside it is valid
         * in both the version it currently has and the version the implementation uses.
         * In that case, the WriteableTSDataFile implementation may opt to add
         * new records to that file.
         * Otherwise, it will have to start a new file to write records to.
         * @return True if the file version can be set to the latest version, false otherwise.
         */
        public boolean isUpgradable() {
            return !isGzipped() && Const.isUpgradable(getVersionMajor(), getVersionMinor());
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 67 * hash + this.version_minor_;
            hash = 67 * hash + this.version_major_;
            hash = 67 * hash + Objects.hashCode(this.begin_);
            hash = 67 * hash + Objects.hashCode(this.end_);
            hash = 67 * hash + Objects.hashCode(this.filename_);
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
            final MetaData other = (MetaData) obj;
            if (this.version_minor_ != other.version_minor_) {
                return false;
            }
            if (this.version_major_ != other.version_major_) {
                return false;
            }
            if (!Objects.equals(this.begin_, other.begin_)) {
                return false;
            }
            if (!Objects.equals(this.end_, other.end_)) {
                return false;
            }
            if (!Objects.equals(this.filename_, other.filename_)) {
                return false;
            }
            if (!Objects.equals(this.is_gzipped_, other.is_gzipped_)) {
                return false;
            }
            return true;
        }
    }

    private final Path dir_;
    private final List<MetaData> meta_data_;

    /**
     * Comparator, in order to sort MetaData based on the end timestamp.
     * @param x One of the MetaData to compare.
     * @param y Another one of the MetaData to compare.
     * @return Comparator result.
     */
    private static int metadata_end_cmp_(MetaData x, MetaData y) {
        return x.getEnd().compareTo(y.getEnd());
    }

    /**
     * Construct a new instance with the given directory and metadata.
     * @param dir The directory used during directory scan.
     * @param meta_data The meta data of files in the directory.
     */
    private TSDataScanDir(Path dir, List<MetaData> meta_data) {
        dir_ = requireNonNull(dir);
        meta_data_ = unmodifiableList(requireNonNull(meta_data));
    }

    /**
     * Scan a directory for files.  This method reads the entire stream and
     * closes it immediately.
     * @param dir The directory to scan for files.
     * @throws IOException if the directory cannot be listed.
     */
    private static List<Path> file_listing_(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.collect(Collectors.toList());
        }
    }

    /**
     * Scan the directory for valid TSDataFiles.
     * @param dir The directory to scan.
     * @throws IOException if the directory cannot be listed.
     */
    public TSDataScanDir(Path dir) throws IOException {
        this(requireNonNull(dir), file_listing_(requireNonNull(dir)).stream()
                .map(MetaData::fromFile)
                .flatMap((opt_md) -> opt_md.map(Stream::of).orElseGet(Stream::empty))
                .sorted(TSDataScanDir::metadata_end_cmp_)
                .collect(Collectors.toList()));
    }

    /**
     * @return the directory used to create this TSDataScanDir.
     */
    public Path getDir() { return dir_; }
    /**
     * @return list of valid TSDataFiles matching predicates.
     */
    public List<MetaData> getFiles() { return meta_data_; }

    /**
     * Filter the list of files to contain only upgradable files.
     *
     * Note that the current instance is untouched and a new instance is returned.
     * @return A new instance of TSDataScanDir, describing only files that are upgradable.
     */
    public TSDataScanDir filterUpgradable() {
        return new TSDataScanDir(getDir(), getFiles().stream().filter(MetaData::isUpgradable).collect(Collectors.toList()));
    }
}
