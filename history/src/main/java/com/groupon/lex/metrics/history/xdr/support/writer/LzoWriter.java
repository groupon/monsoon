package com.groupon.lex.metrics.history.xdr.support.writer;

import java.io.IOException;
import java.io.OutputStream;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoConstraint;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzopConstants;
import org.anarres.lzo.LzopOutputStream;

public class LzoWriter extends AbstractOutputStreamWriter {
    public static final LzoAlgorithm ALGORITHM = LzoAlgorithm.LZO1X;
    private static final long FLAGS = LzopConstants.F_CRC32_C | LzopConstants.F_CRC32_D | LzopConstants.F_ADLER32_C | LzopConstants.F_ADLER32_D;

    public LzoWriter(FileWriter out, boolean highestCompression) throws IOException {
        super(createLzoOutputStream(newAdapter(out), highestCompression));
    }

    private static OutputStream createLzoOutputStream(OutputStream adapter, boolean highestCompression) throws IOException {
        LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(ALGORITHM, highestCompression ? LzoConstraint.COMPRESSION : null);
        LzopOutputStream lzo = new LzopOutputStream(adapter, compressor, 64 * 1024, FLAGS);
        return lzo;
    }
}
