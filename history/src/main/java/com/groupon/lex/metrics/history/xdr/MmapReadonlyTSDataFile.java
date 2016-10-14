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
import com.groupon.lex.metrics.history.xdr.support.GzipDecodingBufferSupplier;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID1_EXPECT;
import static com.groupon.lex.metrics.history.xdr.support.GzipHeaderConsts.ID2_EXPECT;
import com.groupon.lex.metrics.history.xdr.support.Parser;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import com.groupon.lex.metrics.history.xdr.support.XdrStreamIterator;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.LazyEval;
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
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acplt.oncrpc.OncRpcException;
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
    private final LazyEval<Integer> sizeEval = new LazyEval<>(() -> (int) stream().count());
    private final LazyEval<Boolean> emptyEval = new LazyEval<>(() -> !stream().findAny().isPresent());

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
        LOG.log(Level.FINE, "instantiated: version={0}.{1} begin={2}, end={3}", new Object[]{version_major(version_), version_minor(version_), begin_, end_});
    }

    public static MmapReadonlyTSDataFile open(Path file) throws IOException {
        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.READ)) {
            return new MmapReadonlyTSDataFile(fd.map(FileChannel.MapMode.READ_ONLY, 0, fd.size()));
        }
    }

    @Override
    public DateTime getBegin() {
        return begin_;
    }

    @Override
    public DateTime getEnd() {
        return end_;
    }

    @Override
    public short getMajor() {
        return version_major(version_);
    }

    @Override
    public short getMinor() {
        return version_minor(version_);
    }

    @Override
    public long getFileSize() {
        return data_.limit();
    }

    @Override
    public boolean canAddSingleRecord() {
        return !is_gzippped_;
    }

    @Override
    public boolean isOptimized() {
        return false;
    }

    private ByteBuffer getReadonlyData() {
        return data_.asReadOnlyBuffer();
    }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        try {
            if (is_gzippped_)
                return new GzipTsvIterator();
            else
                return new TsvIterator();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "unable to decode file", ex);
            return Collections.emptyIterator();
        }
    }

    @Override
    public boolean add(TimeSeriesCollection tsv) {
        throw new UnsupportedOperationException("add");
    }

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.empty();
    }

    @Override
    public int size() {
        return sizeEval.get();
    }

    @Override
    public boolean isEmpty() {
        return emptyEval.get();
    }
}
