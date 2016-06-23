/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.v0.xdr.ToXdr;
import com.groupon.lex.metrics.history.v0.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.MAGIC;
import static com.groupon.lex.metrics.history.xdr.Const.version_from_majmin;
import com.groupon.lex.metrics.history.xdr.tsfile_mimeheader;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import org.dcache.xdr.Xdr;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class FileSupport0 implements FileSupport.Writer {
    @Override
    public ByteBuffer create_file(List<? extends TimeSeriesCollection> tsdata, short minor) throws IOException {
        final DateTime begin = tsdata.stream().map(TimeSeriesCollection::getTimestamp).min(Comparator.naturalOrder()).orElseGet(() -> new DateTime(DateTimeZone.UTC));
        final DateTime end = tsdata.stream().map(TimeSeriesCollection::getTimestamp).max(Comparator.naturalOrder()).orElse(begin);

        Xdr xdr = new Xdr(Xdr.INITIAL_XDR_SIZE);
        xdr.beginEncoding();
        // Write mime header.
        {
            tsfile_mimeheader hdr = new tsfile_mimeheader();
            hdr.magic = MAGIC;
            hdr.version_number = version_from_majmin((short)0, minor);
            hdr.xdrEncode(xdr);
        }
        // Write header describing the data range.
        {
            tsfile_header hdr = new tsfile_header();
            hdr.first = ToXdr.timestamp(begin);
            hdr.last = ToXdr.timestamp(end);
            hdr.xdrEncode(xdr);
        }
        for (TimeSeriesCollection tsd : tsdata)
            ToXdr.datapoints(tsd).xdrEncode(xdr);
        xdr.endEncoding();

        return xdr.asBuffer().toByteBuffer();
    }
}
