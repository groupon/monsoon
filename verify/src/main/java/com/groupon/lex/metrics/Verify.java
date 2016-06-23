/*
 * Copyright (c) 2016, Groupon, Inc.
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
package com.groupon.lex.metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

class Verify {
    public static final int EX_USAGE = 64;  // From sysexits.h
    public static final int EX_TEMPFAIL = 75;  // From sysexits.h

    @Option(name="-h", usage="print usage instructions")
    private boolean help = false;

    @Option(name="-r", usage="also process import statements")
    private boolean recursive = false;

    @Option(name="-p", usage="print configuration file upon success")
    private boolean print = false;

    @Argument(metaVar="files...", usage="files to verify")
    private List<String> files = new ArrayList<>();

    private static void print_usage_and_exit_(CmdLineParser parser) {
        System.err.println("java -jar monsoon-verify.jar [options...] files...");
        parser.printUsage(System.err);
        System.exit(EX_TEMPFAIL);
        /* UNREACHABLE */
    }

    /**
     * Initialize the verifier, using command-line arguments.
     * @param args The command-line arguments passed to the program.
     */
    public Verify(String[] args) {
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
        if (files.isEmpty())
            System.exit(EX_USAGE);
    }

    /**
     * Perform validation.
     *
     * Prints to stdout.
     */
    public void run() {
        boolean validation_fail = false;
        boolean arg_seen = true;
        for (String file : files) {
            final Report v = new Report(new File(file).getAbsoluteFile(), recursive);
            if (v.hasErrors()) validation_fail = true;
            if (arg_seen) {
                arg_seen = false;
            } else {
                System.out.println(Stream.generate(() -> "-").limit(72).collect(Collectors.joining()));
            }
            System.out.print(v.configString()
                    .filter((x) -> print)
                    .orElseGet(() -> v.toString()));
        }

        if (validation_fail) System.exit(EX_TEMPFAIL);
    }

    public static void main(String[] args) {
        // Dial down the log spam.
        Logger.getLogger("com.groupon.lex").setLevel(Level.SEVERE);

        new Verify(args).run();
    }
}
