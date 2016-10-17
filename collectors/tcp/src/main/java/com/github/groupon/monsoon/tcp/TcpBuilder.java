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
package com.github.groupon.monsoon.tcp;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.ResolverGroupGenerator;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.builders.collector.AcceptAsPath;
import com.groupon.lex.metrics.builders.collector.AcceptTagSet;
import com.groupon.lex.metrics.builders.collector.CollectorBuilder;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class TcpBuilder implements CollectorBuilder, AcceptAsPath, AcceptTagSet {
    private SimpleGroupPath asPath;
    private NameBoundResolver tagSet;

    @Override
    public ResolverGroupGenerator build(EndpointRegistration er) throws Exception {
        final Set<Any2<Integer, String>> keySet = tagSet.getKeys().collect(Collectors.toSet());
        if (!keySet.containsAll(Arrays.asList(Any2.right("host"), Any2.right("port"))))
            throw new IllegalArgumentException("'host' and 'port' arguments must be supplied");

        return new ResolverGroupGenerator(tagSet, new Constructor(getAsPath()));
    }

    @RequiredArgsConstructor
    private static class Constructor implements ResolverGroupGenerator.GroupGeneratorFactory {
        private final SimpleGroupPath pathArg;

        @Override
        public GroupGenerator create(NamedResolverMap args) throws Exception {
            final InetSocketAddress addr = new InetSocketAddress(args.getString("host"), args.getInteger("port"));
            return new TcpCollector(addr, args.getGroupName(pathArg));
        }
    }
}
