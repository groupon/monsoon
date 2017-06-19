package com.groupon.lex.metrics.history.v2;

import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.xdr.support.reader.FileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.GzipReader;
import com.groupon.lex.metrics.history.xdr.support.reader.LzoReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SnappyReader;
import com.groupon.lex.metrics.history.xdr.support.writer.FileWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.GzipWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.LzoWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SnappyWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.stream.Collectors;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Localizable;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

public enum Compression {
    NONE("none", 0, in -> in, (out, hc) -> out),
    GZIP("gzip", header_flags.GZIP, in -> new GzipReader(in), (out, hc) -> new GzipWriter(out)),
    SNAPPY("snappy", header_flags.SNAPPY, in -> new SnappyReader(in), (out, hc) -> new SnappyWriter(out)),
    LZO("lzo", header_flags.LZO_1X1, in -> new LzoReader(in), (out, hc) -> new LzoWriter(out, hc));

    public static final Compression DEFAULT_APPEND = GZIP;
    public static final Compression DEFAULT_OPTIMIZED = GZIP;

    public final String humanName;
    public final int compressionFlag;
    private final WrapReaderFunctor reader;
    private final WrapWriterFunctor writer;

    private Compression(String humanName, int compressionFlag, WrapReaderFunctor reader, WrapWriterFunctor writer) {
        this.humanName = humanName;
        this.compressionFlag = compressionFlag;
        this.reader = requireNonNull(reader);
        this.writer = requireNonNull(writer);
    }

    public FileReader wrap(FileReader reader) throws IOException {
        return this.reader.wrap(reader);
    }

    public FileWriter wrap(FileWriter writer, boolean highestCompression) throws IOException {
        return this.writer.wrap(writer, highestCompression);
    }

    private static interface WrapReaderFunctor {
        public FileReader wrap(FileReader in) throws IOException;
    }

    private static interface WrapWriterFunctor {
        public FileWriter wrap(FileWriter out, boolean highestCompression) throws IOException;
    }

    public static Compression fromFlags(int flags) {
        int cmpFlag = flags & header_flags.COMPRESSION_MASK;
        for (Compression cmprs : values()) {
            if (cmprs.compressionFlag == cmpFlag)
                return cmprs;
        }
        throw new IllegalArgumentException("unrecognized compression 0x" + Integer.toHexString(cmpFlag));
    }

    public static class CompressionOptionHandler extends OptionHandler<Compression> {
        private static final String COMMA_SEPARATED_VALID_OPTIONS = Arrays.stream(Compression.values())
                .map(cmp -> cmp.humanName)
                .collect(Collectors.joining(", "));
        private static final Localizable ILLEGAL_COMPRESSION = new InvalidOperand();

        public CompressionOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Compression> setter) {
            super(parser, option, setter);
        }

        @Override
        public int parseArguments(Parameters prmtrs) throws CmdLineException {
            final String valueStr = prmtrs.getParameter(0);
            final Optional<Compression> compress = Arrays.stream(Compression.values())
                    .filter(cmp -> cmp.humanName.equals(valueStr))
                    .findFirst();
            if (compress.isPresent()) {
                setter.addValue(compress.get());
                return 1;
            }
            throw new CmdLineException(owner, ILLEGAL_COMPRESSION, valueStr, COMMA_SEPARATED_VALID_OPTIONS);
        }

        @Override
        public String getDefaultMetaVariable() {
            return "COMPRESS";
        }

        @Override
        protected String print(Compression v) {
            return v.humanName;
        }

        private static class InvalidOperand implements Localizable {
            private static final String MESSAGE = "Invalid compression {0}, expecting one of {1}";

            @Override
            public String formatWithLocale(Locale locale, Object... args) {
                return format(args); // XXX: maybe add localization?
            }

            @Override
            public String format(Object... args) {
                return MessageFormat.format(MESSAGE, args);
            }
        }
    }
}
