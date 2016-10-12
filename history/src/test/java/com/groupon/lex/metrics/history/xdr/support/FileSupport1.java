/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.TSDataVersionDispatch;
import com.groupon.lex.metrics.history.v1.xdr.FromXdr;
import com.groupon.lex.metrics.history.v1.xdr.ToXdr;
import com.groupon.lex.metrics.history.v1.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.MAGIC;
import static com.groupon.lex.metrics.history.xdr.Const.version_from_majmin;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.FileWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.GzipWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.history.xdr.tsfile_mimeheader;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Comparator;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author ariane
 */
public class FileSupport1 implements FileSupport.Writer {
    @Override
    public void create_file(TSDataVersionDispatch.Releaseable<FileChannel> fd, Collection<? extends TimeSeriesCollection> tsdata, boolean compress) throws IOException {
        try {
            final DateTime begin = tsdata.stream().map(TimeSeriesCollection::getTimestamp).min(Comparator.naturalOrder()).orElseGet(() -> new DateTime(DateTimeZone.UTC));
            final DateTime end = tsdata.stream().map(TimeSeriesCollection::getTimestamp).max(Comparator.naturalOrder()).orElse(begin);
            final ToXdr to_xdr_ = new ToXdr(new FromXdr());

            try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(wrapCompress(new FileChannelWriter(fd.get(), 0), compress))) {
                xdr.beginEncoding();
                // Write mime header.
                {
                    tsfile_mimeheader hdr = new tsfile_mimeheader();
                    hdr.magic = MAGIC;
                    hdr.version_number = version_from_majmin(getMajor(), getMinor());
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
                    to_xdr_.data(tsd).xdrEncode(xdr);
                xdr.endEncoding();
            }
        } catch (OncRpcException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public short getMajor() { return (short)1; }
    @Override
    public short getMinor() { return (short)0; }

    private static FileWriter wrapCompress(FileWriter writer, boolean compress) throws IOException {
        if (compress) writer = new GzipWriter(writer);
        return writer;
    }
}
