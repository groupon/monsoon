package com.groupon.lex.metrics.history.v1.xdr;

import com.groupon.lex.metrics.history.xdr.support.Parser;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import org.dcache.xdr.XdrBufferDecodingStream;

public class Parser1 implements Parser<tsfile_header> {
    private final FromXdr from_xdr_ = new FromXdr();

    @Override
    public BeginEnd header(XdrBufferDecodingStream s) throws IOException {
        final tsfile_header hdr = new tsfile_header(s);
        return new BeginEnd(FromXdr.timestamp(hdr.first), FromXdr.timestamp(hdr.last));
    }

    @Override
    public TimeSeriesCollection apply(XdrBufferDecodingStream data) throws IOException {
        return from_xdr_.data(new tsfile_data(data));
    }
}
