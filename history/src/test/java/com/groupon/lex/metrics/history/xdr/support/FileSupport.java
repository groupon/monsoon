package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.TSDataVersionDispatch.Releaseable;
import com.groupon.lex.metrics.history.xdr.Const;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import static java.lang.Math.sqrt;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.util.Collection;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Utilities for creating and compressing TSData files.
 * @author ariane
 */
@RequiredArgsConstructor
public class FileSupport {
    public static interface Writer {
        public void create_file(Releaseable<FileChannel> fd, Collection<? extends TimeSeriesCollection> tsdata, boolean compress) throws IOException;
        public short getMajor();
        public short getMinor();
    }

    public static final Writer NO_WRITER = new Writer() {
        @Override
        public void create_file(Releaseable<FileChannel> fd, Collection<? extends TimeSeriesCollection> tsdata, boolean compress) throws IOException {
            throw new UnsupportedOperationException("Not a writer.");
        }

        @Override
        public short getMajor() {
            return Const.MAJOR;
        }

        @Override
        public short getMinor() {
            return Const.MINOR;
        }
    };

    @NonNull
    private final Writer writer;
    @Getter
    private final boolean compressed;
    public final DateTime NOW = new DateTime(DateTimeZone.UTC);

    public short getMajor() { return writer.getMajor(); }
    public short getMinor() { return writer.getMinor(); }

    /** Create a file. */
    public void create_file(Path file, Collection<? extends TimeSeriesCollection> tsdata) throws IOException {
        try (Releaseable<FileChannel> fd = new Releaseable<>(FileChannel.open(file, READ, WRITE, CREATE))) {
            writer.create_file(fd, tsdata, compressed);
        } catch (IOException | RuntimeException ex) {
            Files.delete(file);
            throw ex;
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
                    final DateTime now = NOW.plusSeconds(5 * i);
                    final Random rnd = new Random(Integer.hashCode(i));  // Deterministic RNG.

                    final Stream<TimeSeriesValue> tsv_stream = group_names.stream()
                            .map(name -> {
                                return new MutableTimeSeriesValue(
                                        now,
                                        name,
                                        metric_names.stream(),
                                        Function.identity(),
                                        (ignored) -> MetricValue.fromIntValue(rnd.nextLong()));
                            });

                    return new SimpleTimeSeriesCollection(now, tsv_stream);
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
