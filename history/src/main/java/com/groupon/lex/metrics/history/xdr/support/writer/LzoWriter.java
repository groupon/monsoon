package com.groupon.lex.metrics.history.xdr.support.writer;

import com.groupon.lex.metrics.history.xdr.support.reader.LzoReader;
import java.io.IOException;
import java.io.OutputStream;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;

public class LzoWriter extends AbstractOutputStreamWriter {
    public static final LzoAlgorithm ALGORITHM = LzoReader.ALGORITHM;

    public LzoWriter(FileWriter out) throws IOException {
        super(createLzoOutputStream(newAdapter(out)));
    }

    private static OutputStream createLzoOutputStream(OutputStream adapter) {
        LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(ALGORITHM, null);
        LzoOutputStream lzo = new LzoOutputStream(adapter, compressor, 64 * 1024);
        return lzo;
    }
}
