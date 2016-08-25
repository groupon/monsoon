package com.groupon.lex.metrics.history.v0.xdr;

import com.groupon.lex.metrics.history.xdr.support.Parser;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import org.acplt.oncrpc.OncRpcException;
import org.dcache.xdr.XdrBufferDecodingStream;

public class Parser0 implements Parser<tsfile_header> {
    @Override
    public BeginEnd header(XdrBufferDecodingStream s) throws IOException {
        final tsfile_header hdr;
        try {
            hdr = new tsfile_header(s);
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }
        return new BeginEnd(FromXdr.timestamp(hdr.first), FromXdr.timestamp(hdr.last));
    }

    @Override
    public TimeSeriesCollection apply(XdrBufferDecodingStream data) throws IOException {
        try {
            return FromXdr.datapoints(new tsfile_datapoint(data));
        } catch (OncRpcException ex) {
            throw new IOException("RPC decoding error", ex);
        }
    }
}
