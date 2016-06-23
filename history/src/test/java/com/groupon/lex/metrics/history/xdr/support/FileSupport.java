package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Utilities for creating and compressing TSData files.
 * @author ariane
 */
public class FileSupport {
    public static interface Writer {
        public ByteBuffer create_file(List<? extends TimeSeriesCollection> tsdata, short minor) throws IOException;
    }

    private final short major;
    private final short minor;
    public final DateTime NOW = new DateTime(DateTimeZone.UTC);

    public short getMajor() { return major; }
    public short getMinor() { return minor; }

    public FileSupport(short major, short minor) {
        this.major = major;
        this.minor = minor;
    }

    /** Create a 0.0 file. */
    public void create_file(Path file, List<? extends TimeSeriesCollection> tsdata) throws IOException {
        final Writer writer;
        switch(major) {
            default:
                throw new IOException("major " + major + " not recognized");
            case 0:
                writer = new FileSupport0();
                break;
            case 1:
                writer = new FileSupport1();
                break;
        }

        try (FileChannel fd = FileChannel.open(file, READ, WRITE, CREATE)) {
            fd.write(writer.create_file(tsdata, minor));
        } catch (IOException | RuntimeException ex) {
            Files.delete(file);
            throw ex;
        }
    }

    /** Compress file in place. */
    public static void compress_file(Path file) throws IOException {
        final ByteBuffer file_data;
        try (FileChannel fd = FileChannel.open(file, READ, WRITE)) {
            file_data = ByteBuffer.allocateDirect((int)fd.size());
            fd.read(file_data);
            file_data.flip();

            // Write data in place.
            fd.position(0);
            fd.truncate(0);

            try (WritableByteChannel out = Channels.newChannel(new GZIPOutputStream(Channels.newOutputStream(fd)))) {
                out.write(file_data);
            }
        }
    }

    /** Generates an endless stream of TimeSeriesCollections. */
    public Stream<TimeSeriesCollection> create_tsdata(int width) {
        SimpleGroupPath base_path = new SimpleGroupPath("foo", "bar");

        return Stream.generate(
                new Supplier<Integer>() {
                    private int idx = 0;

                    @Override
                    public Integer get() {
                        return idx++;
                    }
                })
                .map(new Function<Integer, TimeSeriesCollection>() {
                    public FileTimeSeriesCollection apply(Integer i) {
                        final DateTime now = NOW.plusMinutes(i);
                        Map<MetricName, MetricValue> metrics = singletonMap(new MetricName("x"), MetricValue.fromIntValue(i));

                        final Stream<TimeSeriesValue> tsv_stream = Stream.generate(
                                new Supplier<Integer>() {
                                    private int idx = 0;

                                    @Override
                                    public Integer get() {
                                        return idx++;
                                    }
                                })
                                .map(idx -> {
                                    return new MutableTimeSeriesValue(now,
                                            new GroupName(base_path, new Tags(singletonMap("instance", MetricValue.fromIntValue(idx)))),
                                            metrics);
                                });

                        return new FileTimeSeriesCollection(now, tsv_stream.limit(width));
                    }
                });
    }
}
