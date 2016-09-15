/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.TSData;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import static com.groupon.lex.metrics.history.xdr.Const.version_major;
import static com.groupon.lex.metrics.history.xdr.Const.version_minor;
import com.groupon.lex.metrics.history.xdr.support.FileIterator;
import com.groupon.lex.metrics.history.xdr.support.GzipDecodingBufferSupplier;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID1_EXPECT;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID2_EXPECT;
import com.groupon.lex.metrics.history.xdr.support.Parser;
import com.groupon.lex.metrics.history.xdr.support.XdrStreamIterator;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import static java.util.Objects.requireNonNull;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acplt.oncrpc.OncRpcException;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Getter;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public final class MmapReadonlyTSDataFile implements TSData {
    private static final Logger LOG = Logger.getLogger(MmapReadonlyTSDataFile.class.getName());
    private final ByteBuffer data_;
    private final DateTime begin_, end_;
    private final int version_;
    private final boolean is_gzippped_;
    @Getter
    private final boolean ordered, unique;

    private class TsvIterator extends XdrStreamIterator {  // non-static, to keep weak reference to MmapReadonlyTSDataFile alive.
        public TsvIterator() throws IOException {
            super(getReadonlyData());
        }
    }

    private class GzipTsvIterator extends XdrStreamIterator {  // non-static, to keep weak reference to MmapReadonlyTSDataFile alive.
        public GzipTsvIterator() throws IOException {
            super(wrap_gzip_(getReadonlyData()));
        }
    }

    private static XdrBufferDecodingStream wrap_gzip_(ByteBuffer data) throws IOException {
        return new XdrBufferDecodingStream(new GzipDecodingBufferSupplier(new BufferSupplier() {
            @Override
            public void load(ByteBuffer buf) throws IOException {
                while (buf.hasRemaining() && data.hasRemaining()) {
                    final int len = Integer.min(buf.remaining(), data.remaining());
                    if (buf.hasArray()) {
                        data.get(buf.array(), buf.arrayOffset() + buf.position(), len);
                        buf.position(buf.position() + len);
                    } else {
                        byte[] tmp = new byte[len];
                        data.get(tmp);
                        buf.put(tmp);
                    }
                }
            }

            @Override
            public boolean atEof() {
                return !data.hasRemaining();
            }
        }));
    }

    public MmapReadonlyTSDataFile(ByteBuffer data) throws IOException {
        data_ = requireNonNull(data);
        data_.order(ByteOrder.BIG_ENDIAN);

        final byte id1, id2;
        {
            ByteBuffer tmp_buf = data_.asReadOnlyBuffer();
            id1 = tmp_buf.get();
            id2 = tmp_buf.get();
        }
        is_gzippped_ = (id1 == ID1_EXPECT && id2 == ID2_EXPECT);

        final XdrBufferDecodingStream stream;
        if (is_gzippped_)
            stream = wrap_gzip_(data_.asReadOnlyBuffer());
        else
            stream = new XdrBufferDecodingStream(data_.asReadOnlyBuffer());
        try {
            version_ = validateHeaderOrThrow(stream);
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }

        final Parser.BeginEnd header = Parser.fromVersion(version_).header(stream);
        begin_ = header.getBegin();
        end_ = header.getEnd();
        LOG.log(Level.INFO, "instantiated: version={0}.{1} begin={2}, end={3}", new Object[]{version_major(version_), version_minor(version_), begin_, end_});

        /*
         * Check if the collection is ordered and unique.
         */
        final Iterator<TimeSeriesCollection> iter = iterator();
        if (!iter.hasNext()) {
            ordered = true;
            unique = true;
        } else {
            boolean uniqueLoopInvariant = true;
            boolean orderedLoopInvariant = true;

            DateTime ts = iter.next().getTimestamp();
            while (iter.hasNext()) {
                final DateTime nextTs = iter.next().getTimestamp();

                if (ts.equals(nextTs))
                    uniqueLoopInvariant = false;
                if (nextTs.isBefore(ts))
                    orderedLoopInvariant = false;
            }

            ordered = orderedLoopInvariant;
            unique = uniqueLoopInvariant;
        }
    }

    public static MmapReadonlyTSDataFile open(Path file) throws IOException {
        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            return new MmapReadonlyTSDataFile(fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size()));
        }
    }

    @Override
    public boolean isGzipped() { return is_gzippped_; }
    @Override
    public DateTime getBegin() { return begin_; }
    @Override
    public DateTime getEnd() { return end_; }
    @Override
    public short getMajor() { return version_major(version_); }
    @Override
    public short getMinor() { return version_minor(version_); }
    @Override
    public long getFileSize() { return data_.limit(); }
    private ByteBuffer getReadonlyData() { return data_.asReadOnlyBuffer(); }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        try {
            Iterator<TimeSeriesCollection> iter;
            if (is_gzippped_)
                iter = new GzipTsvIterator();
            else
                iter = new TsvIterator();

            if (!isOrdered()) {
                List<TimeSeriesCollection> data = new ArrayList<>();
                iter.forEachRemaining(data::add);
                Collections.sort(data, Comparator.comparing(TimeSeriesCollection::getTimestamp));
                iter = data.iterator();
            }
            if (!isUnique()) {
                iter = new FileIterator(iter);
            }

            return iter;
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "unable to decode file", ex);
            return Collections.emptyIterator();
        }
    }

    @Override
    public boolean add(TimeSeriesCollection tsv) {
        throw new UnsupportedOperationException("add");
    }
}
