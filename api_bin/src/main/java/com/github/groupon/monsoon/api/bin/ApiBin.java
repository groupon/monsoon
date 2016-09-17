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
package com.github.groupon.monsoon.api.bin;

import com.groupon.lex.metrics.api.ApiServer;
import com.groupon.lex.metrics.api.endpoints.ExprEval;
import com.groupon.lex.metrics.api.endpoints.ExprEvalGraphServlet;
import com.groupon.lex.metrics.api.endpoints.ExprValidate;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.monsoon.remote.history.Client;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.SneakyThrows;
import org.acplt.oncrpc.OncRpcException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class ApiBin {
    private static final Logger LOG = Logger.getLogger(ApiBin.class.getName());
    public static final int EX_USAGE = 64;  // From sysexits.h
    public static final int EX_TEMPFAIL = 75;  // From sysexits.h

    @Option(name="-h", usage="print usage instructions")
    private boolean help = false;

    @Option(name="-R", metaVar="history-server", usage="use remote history server", handler=RHistClientSocketOptionHandler.class)
    private InetSocketAddress addr = new InetSocketAddress("localhost", Client.DEFAULT_PORT);

    @Option(name="-L", metaVar="listen", usage="IP/port on which the webserver listens", handler=ApiServerSocketOptionHandler.class)
    private List<InetSocketAddress> listen = new ArrayList<InetSocketAddress>() {{
        add(new InetSocketAddress(anyAddr(), 9998));
    }};

    private static void print_usage_and_exit_(CmdLineParser parser) {
        System.err.println("java -jar monsoon-api-bin.jar [options]");
        parser.printUsage(System.err);
        System.exit(EX_TEMPFAIL);
        /* UNREACHABLE */
    }

    @SneakyThrows(UnknownHostException.class)
    private static InetAddress anyAddr() {
        return InetAddress.getByAddress(new byte[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
    }

    public ApiBin(String[] args) {
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
    }

    public void run() throws OncRpcException, IOException, Exception {
        final CollectHistory history = createHistory();
        final ApiServer api = new ApiServer(listen);

        api.addEndpoint("/monsoon/eval", new ExprEval(history));
        api.addEndpoint("/monsoon/eval/validate", new ExprValidate());
        api.addEndpoint("/monsoon/eval/gchart", new ExprEvalGraphServlet(history));

        api.start();
        Runtime.getRuntime().addShutdownHook(new Thread(api::close));
    }

    public static void main(String[] args) {
        try {
            new ApiBin(args).run();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Unable to start: ", ex);
            System.exit(EX_TEMPFAIL);
        }
    }

    private CollectHistory createHistory() throws OncRpcException, IOException {
        return new Client(addr.getAddress(), addr.getPort());
    }
}
