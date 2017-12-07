package com.github.groupon.monsoon.history.influx;

import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.history.xdr.DirCollectHistory;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import org.influxdb.InfluxDBFactory;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class FileConvert {
    private static final Logger LOG = Logger.getLogger(FileConvert.class.getName());
    public static final int EX_USAGE = 64;  // From sysexits.h
    public static final int EX_TEMPFAIL = 75;  // From sysexits.h

    @Option(name = "-h", usage = "print usage instructions", help = true)
    private boolean help = false;

    @Option(name = "-v", usage = "verbose")
    private boolean verbose = false;

    @Option(name = "--influx_dst", usage = "Influx URL to where data will be written")
    private String influxDst = "http://localhost:8086";

    @Option(name = "--database", usage = "Influx database to where data will be written")
    private String database;

    @Option(name = "--skip_db_check", usage = "Don't check if the database exists (useful if you're using telegraf for instance)")
    private boolean skipDatabaseExistCheck = false;

    @Argument(metaVar = "/src/dir", usage = "path: which dir contains source files", index = 0)
    private String srcdir;

    @NonNull
    private final Path srcdir_path_;

    private static void print_usage_and_exit_(CmdLineParser parser) {
        System.err.println("java -jar monsoon-extra-influx-copy.jar [options] /src/dir");
        parser.printUsage(System.err);
        System.exit(EX_TEMPFAIL);
        /* UNREACHABLE */
    }

    public FileConvert(String[] args) {
        final CmdLineParser parser = new CmdLineParser(this, ParserProperties.defaults().withUsageWidth(80));
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            print_usage_and_exit_(parser);
            /* UNREACHABLE */
        }

        // If help is requested, simply print that.
        if (help) {
            print_usage_and_exit_(parser);
            /* UNREACHABLE */
        }

        // If verbose mode is requested, dial up the log spam.
        if (verbose) {
            Logger.getLogger("com.groupon.lex").setLevel(Level.INFO);
            Logger.getLogger("com.github.groupon.monsoon").setLevel(Level.INFO);
        }

        // If there are no files, comlain with a non-zero exit code.
        if (srcdir == null)
            System.exit(EX_USAGE);

        srcdir_path_ = FileSystems.getDefault().getPath(srcdir);
    }

    public void run() throws IOException, Exception {
        final CollectHistory src = new DirCollectHistory(srcdir_path_);
        try {
            final CollectHistory dst = new InfluxHistory(InfluxDBFactory.connect(influxDst).enableGzip(), database, !skipDatabaseExistCheck);
            try {
                copy(src, dst);
            } finally {
                if (dst instanceof AutoCloseable)
                    ((AutoCloseable) dst).close();
            }
        } finally {
            if (src instanceof AutoCloseable)
                ((AutoCloseable) src).close();
        }
    }

    public static void main(String[] args) throws Exception {
        // Dial down the log spam.
        Logger.getLogger("com.groupon.lex").setLevel(Level.WARNING);
        Logger.getLogger("com.github.groupon.monsoon").setLevel(Level.WARNING);

        try {
            new FileConvert(args).run();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Unable to complete copy operation: ", ex);
            System.exit(EX_TEMPFAIL);
        }
    }

    private static void copy(CollectHistory src, CollectHistory dst) {
        dst.addAll(src.stream().iterator());
    }
}
