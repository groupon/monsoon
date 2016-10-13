package com.groupon.lex.metrics.history.xdr.support.reader;

import java.io.IOException;
import org.iq80.snappy.SnappyFramedInputStream;

public class SnappyReader extends AbstractInputStreamReader {
    public SnappyReader(FileReader in, boolean validateAllRead) throws IOException {
        super(new SnappyFramedInputStream(newAdapter(in), true), validateAllRead);
    }

    public SnappyReader(FileReader in) throws IOException {
        super(new SnappyFramedInputStream(newAdapter(in), true));
    }
}
