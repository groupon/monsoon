package com.groupon.lex.metrics.history.xdr.support.reader;

import java.io.IOException;
import java.io.InputStream;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoDecompressor;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzoLibrary;

public class LzoReader extends AbstractInputStreamReader {
    public static final LzoAlgorithm ALGORITHM = LzoAlgorithm.LZO1X;

    public LzoReader(FileReader in, boolean validateAllRead) throws IOException {
        super(createLzoInputStream(newAdapter(in)), validateAllRead);
    }

    public LzoReader(FileReader in) throws IOException {
        super(createLzoInputStream(newAdapter(in)));
    }

    private static InputStream createLzoInputStream(InputStream adapter) {
        LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(ALGORITHM, null);
        LzoInputStream lzo = new LzoInputStream(adapter, decompressor);
        lzo.setInputBufferSize(64 * 1024);
        return lzo;
    }
}
