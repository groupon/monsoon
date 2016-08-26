package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.v0.xdr.Parser0;
import com.groupon.lex.metrics.history.v1.xdr.Parser1;
import com.groupon.lex.metrics.history.xdr.Const;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import lombok.Value;
import org.joda.time.DateTime;

public interface Parser<HeaderType> {
    public BeginEnd header(XdrBufferDecodingStream s) throws IOException;
    public TimeSeriesCollection apply(XdrBufferDecodingStream s) throws IOException;

    @Value
    public static class BeginEnd {
        private DateTime begin;
        private DateTime end;

        @Override
        public String toString() { return begin + " - " + end; }
    }

    public static Parser<?> fromVersion(int ver) throws IOException {
        switch (Const.version_major(ver)) {
        default:
            throw new IOException("no parser for file version " + Const.version_major(ver) + "." + Const.version_minor(ver));
        case 0:
            return new Parser0();
        case 1:
            return new Parser1();
        }
    }
}
