package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import com.groupon.lex.metrics.history.xdr.Const;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import static java.util.Objects.requireNonNull;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acplt.oncrpc.OncRpcException;

/**
 *
 * @author ariane
 */
public class XdrStreamIterator implements Iterator<TimeSeriesCollection> {
    private static final Logger LOG = Logger.getLogger(XdrStreamIterator.class.getName());
    private final XdrBufferDecodingStream data_;
    private int counter_ = 0;
    private final Parser<?> parser_;

    public XdrStreamIterator(XdrBufferDecodingStream data) throws IOException {
        data_ = requireNonNull(data);
        parser_ = skip_header_(data_);
    }

    public XdrStreamIterator(BufferSupplier data) throws IOException {
        this(new XdrBufferDecodingStream(data));
    }

    public XdrStreamIterator(ByteBuffer buf, BufferSupplier data) throws IOException {
        this(new XdrBufferDecodingStream(buf, data));
    }

    public XdrStreamIterator(ByteBuffer data) throws IOException {
        this(new XdrBufferDecodingStream(data));
    }

    private static Parser<?> skip_header_(XdrBufferDecodingStream decoder) throws IOException {
        final int ver;
        try {
            ver = Const.validateHeaderOrThrow(decoder);
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }
        final Parser<?> parser = Parser.fromVersion(ver);
        final Parser.BeginEnd hdr = parser.header(decoder);
        LOG.log(Level.FINE, "header version {0}.{1}: {2}", new Object[]{Const.version_major(ver), Const.version_minor(ver), hdr});
        return parser;
    }

    @Override
    public boolean hasNext() {
        try {
            LOG.log(Level.FINEST, "hasNext: {0}, counter: {1}", new Object[]{!data_.atEof(), counter_});
            return !data_.atEof();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "error accessing data file", ex);
            return true;
        }
    }

    @Override
    public TimeSeriesCollection next() {
        try {
            if (data_.atEof()) throw new NoSuchElementException("EOF");
            ++counter_;
            return parser_.apply(data_);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
