/*
 * Copyright (c) 2016, Ariane van der Steldt
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.monsoon.remote.history.server;

import com.groupon.lex.metrics.history.xdr.DirCollectHistory;
import com.groupon.monsoon.remote.history.CollectHistoryServer;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.server.OncRpcServerTransport;
import org.acplt.oncrpc.server.OncRpcTcpServerTransport;
import org.acplt.oncrpc.server.OncRpcUdpServerTransport;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class RhistMain {
    private static final Logger LOG = Logger.getLogger(RhistMain.class.getName());
    public static final int EX_USAGE = 64;  // From sysexits.h
    public static final int EX_TEMPFAIL = 75;  // From sysexits.h

    @Option(name="-h", usage="print usage instructions")
    private boolean help = false;

    @Option(name="-p", usage="list on given port")
    private int port = CollectHistoryServer.DEFAULT_PORT;

    @Argument(metaVar="/path/to/history/dir", usage="path: which dir contains the history files", index=0)
    private String dir;

    @NonNull
    private final Path path_;

    private static void print_usage_and_exit_(CmdLineParser parser) {
        System.err.println("java -jar monsoon-rhist-server.jar [options] /path/to/history/dir");
        parser.printUsage(System.err);
        System.exit(EX_TEMPFAIL);
        /* UNREACHABLE */
    }

    /**
     * Initialize the verifier, using command-line arguments.
     * @param args The command-line arguments passed to the program.
     */
    public RhistMain(String[] args) {
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

        path_ = FileSystems.getDefault().getPath(dir);
    }

    public void run() throws IOException, OncRpcException {
        CollectHistoryServer server = new CollectHistoryServer(new DirCollectHistory(path_));

        OncRpcUdpServerTransport rpcUdp = new OncRpcUdpServerTransport(server, null, port, server.info, 32768);
        OncRpcTcpServerTransport rpcTcp = new OncRpcTcpServerTransport(server, null, port, server.info, 32768);
        server.run(new OncRpcServerTransport[]{ rpcUdp, rpcTcp });
        rpcTcp.close();
        rpcUdp.close();
    }

    public static void main(String[] args) {
        try {
            new RhistMain(args).run();
        } catch (IOException | OncRpcException ex) {
            LOG.log(Level.SEVERE, "Unable to complete copy operation: ", ex);
            System.exit(EX_TEMPFAIL);
        }
    }
}
