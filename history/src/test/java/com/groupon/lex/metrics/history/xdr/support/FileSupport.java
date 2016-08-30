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
import static java.lang.Math.sqrt;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Collection;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

    /** Create a file. */
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
    public StreamedCollection<TimeSeriesCollection> create_tsdata(int width) {
        int metric_width = (int)sqrt(width) + 1;
        int group_width = (width + metric_width - 1) / metric_width;

        final SimpleGroupPath base_path = SimpleGroupPath.valueOf("foo", "bar");
        final List<GroupName> group_names = Stream.generate(new CounterSupplier())
                .limit(group_width)
                .map(idx -> Tags.valueOf(singletonMap("instance", MetricValue.fromIntValue(idx))))
                .map(tags -> GroupName.valueOf(base_path, tags))
                .collect(Collectors.toList());
        final List<MetricName> metric_names = Stream.generate(new CounterSupplier())
                .limit(metric_width)
                .map(i -> (MetricName.valueOf("x", String.valueOf(i))))
                .collect(Collectors.toList());

        return new StreamedCollection<>(() -> Stream.generate(new CounterSupplier()))
                .map((Integer i) -> {
                    final DateTime now = NOW.plusMinutes(i);
                    final MetricValue value = MetricValue.fromIntValue(i);

                    final Stream<TimeSeriesValue> tsv_stream = group_names.stream()
                            .map(name -> new MutableTimeSeriesValue(now, name, metric_names.stream(), Function.identity(), (ignored) -> value));

                    return new FileTimeSeriesCollection(now, tsv_stream);
                });
    }

    private static class CounterSupplier implements Supplier<Integer> {
        private int idx = 0;

        @Override
        public Integer get() {
            return idx++;
        }
    }

    public static ByteBuffer createSingleBuffer(Collection<ByteBuffer> bufs) {
        final int totalLength = bufs.stream()
                .mapToInt(ByteBuffer::limit)
                .sum();
        final ByteBuffer out = ByteBuffer.allocateDirect(totalLength);

        bufs.forEach(out::put);
        out.flip();
        return out;
    }
}
