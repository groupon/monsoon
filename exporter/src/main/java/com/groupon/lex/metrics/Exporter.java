package com.groupon.lex.metrics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import com.groupon.lex.metrics.history.xdr.DirCollectHistory;
import com.groupon.lex.metrics.json.Json;
import com.groupon.lex.metrics.lib.BufferedIterator;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.FileSystems;
import lombok.Getter;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;

class Exporter {
    private static final Logger LOG = Logger.getLogger(Exporter.class.getName());
    public static final int EX_USAGE = 64;  // From sysexits.h
    public static final int EX_TEMPFAIL = 75;  // From sysexits.h

    @Option(name="-h", usage="print usage instructions")
    private boolean help = false;

    @Option(name="-P", usage="Pretty printing of JSON")
    private boolean prettyPrint = false;

    @Argument(metaVar="/history/dir", usage="path: which dir contains history files", index=0)
    private String dir;

    @Getter
    private final Path dirPath;

    private static void print_usage_and_exit_(CmdLineParser parser) {
        System.err.println("java -jar monsoon-export.jar [options] /history/dir");
        parser.printUsage(System.err);
        System.exit(EX_TEMPFAIL);
        /* UNREACHABLE */
    }

    public Exporter(String[] args) {
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

        // If there are no files, comlain with a non-zero exit code.
        if (dir == null)
            System.exit(EX_USAGE);

        dirPath = FileSystems.getDefault().getPath(dir);
    }

    private Gson buildGson() {
        GsonBuilder builder = new GsonBuilder();
        if (prettyPrint) builder = builder.setPrettyPrinting();
        return builder.create();
    }

    public void run(Writer writer) throws IOException {
        final Gson gson = buildGson();
        final JsonWriter out = gson.newJsonWriter(writer);

        final DirCollectHistory src = new DirCollectHistory(getDirPath());

        out.beginArray();
        try {
            final Iterator<TimeSeriesCollection> tsdata_iter = BufferedIterator.stream(ForkJoinPool.commonPool(), src.stream()).iterator();
            while (tsdata_iter.hasNext()) {
                final TimeSeriesCollection tsdata = tsdata_iter.next();
                Json.toJson(gson, out, tsdata);
            }
        } finally {
            out.endArray();
        }
    }

    public static void main(String[] args) {
        // Dial down the log spam.
        Logger.getLogger("com.groupon.lex").setLevel(Level.WARNING);

        try {
            new Exporter(args).run(new OutputStreamWriter(System.out));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Unable to complete export operation: ", ex);
            System.exit(EX_TEMPFAIL);
        }
    }
}
