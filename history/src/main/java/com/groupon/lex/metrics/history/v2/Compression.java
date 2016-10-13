package com.groupon.lex.metrics.history.v2;

import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.xdr.support.reader.FileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.GzipReader;
import com.groupon.lex.metrics.history.xdr.support.reader.LzoReader;
import com.groupon.lex.metrics.history.xdr.support.writer.FileWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.GzipWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.LzoWriter;
import java.io.IOException;
import static java.util.Objects.requireNonNull;

public enum Compression {
    NONE(0, in -> in, out -> out),
    GZIP(header_flags.GZIP, in -> new GzipReader(in), out -> new GzipWriter(out)),
    LZO(header_flags.LZO, in -> new LzoReader(in), out -> new LzoWriter(out));

    public static final Compression DEFAULT = LZO;

    public final int compressionFlag;
    private final WrapReaderFunctor reader;
    private final WrapWriterFunctor writer;

    private Compression(int compressionFlag, WrapReaderFunctor reader, WrapWriterFunctor writer) {
        this.compressionFlag = compressionFlag;
        this.reader = requireNonNull(reader);
        this.writer = requireNonNull(writer);
    }

    public FileReader wrap(FileReader reader) throws IOException {
        return this.reader.wrap(reader);
    }

    public FileWriter wrap(FileWriter writer) throws IOException {
        return this.writer.wrap(writer);
    }

    private static interface WrapReaderFunctor {
        public FileReader wrap(FileReader in) throws IOException;
    }

    private static interface WrapWriterFunctor {
        public FileWriter wrap(FileWriter out) throws IOException;
    }

    public static Compression fromFlags(int flags) {
        int cmpFlag = flags & header_flags.COMPRESSION_MASK;
        for (Compression cmprs : values()) {
            if (cmprs.compressionFlag == cmpFlag)
                return cmprs;
        }
        throw new IllegalArgumentException("unrecognized compression 0x" + Integer.toHexString(cmpFlag));
    }
}
