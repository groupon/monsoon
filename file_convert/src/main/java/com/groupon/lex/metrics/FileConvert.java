package com.groupon.lex.metrics;

import com.groupon.lex.metrics.history.xdr.DirCollectHistory;
import com.groupon.lex.metrics.lib.BufferedIterator;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class FileConvert {
    private static final Logger LOG = Logger.getLogger(FileConvert.class.getName());
    public static final int EX_USAGE = 64;  // From sysexits.h
    public static final int EX_TEMPFAIL = 75;  // From sysexits.h

    @Option(name = "-h", usage = "print usage instructions")
    private boolean help = false;

    @Option(name = "-v", usage = "verbose")
    private boolean verbose = false;

    @Argument(metaVar = "/src/dir", usage = "path: which dir contains source files", index = 0)
    private String srcdir;

    @Argument(metaVar = "/dst/dir", usage = "path: where to write new files", index = 1)
    private String dstdir;

    @NonNull
    private final Path srcdir_path_, dstdir_path_;

    private static void print_usage_and_exit_(CmdLineParser parser) {
        System.err.println("java -jar monsoon-file_convert.jar [options] /src/dir /dst/dir");
        parser.printUsage(System.err);
        System.exit(EX_TEMPFAIL);
        /* UNREACHABLE */
    }

    /**
     * Initialize the verifier, using command-line arguments.
     *
     * @param args The command-line arguments passed to the program.
     */
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
        if (verbose)
            Logger.getLogger("com.groupon.lex").setLevel(Level.INFO);

        // If there are no files, comlain with a non-zero exit code.
        if (srcdir == null || dstdir == null)
            System.exit(EX_USAGE);

        srcdir_path_ = FileSystems.getDefault().getPath(srcdir);
        dstdir_path_ = FileSystems.getDefault().getPath(dstdir);
    }

    public void run() throws IOException {
        final DirCollectHistory src = new DirCollectHistory(srcdir_path_);
        final DirCollectHistory dst = new DirCollectHistory(dstdir_path_);
        BufferedIterator.stream(ForkJoinPool.commonPool(), src.stream())
                .forEach(tsc -> {
                    dst.add(tsc);
                    dst.waitPendingTasks();
                });

        // Wait for pending optimization to finish.
        src.waitPendingTasks();
        dst.waitPendingTasks();
    }

    public static void main(String[] args) {
        // Dial down the log spam.
        Logger.getLogger("com.groupon.lex").setLevel(Level.WARNING);

        try {
            new FileConvert(args).run();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Unable to complete copy operation: ", ex);
            System.exit(EX_TEMPFAIL);
        }
    }
}
