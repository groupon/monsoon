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
package com.groupon.lex.metrics.lib;

import static java.util.Collections.unmodifiableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

public class BytesParser {
    public static final Map<Character, Long> SIZE_MAP;

    static {
        Map<Character, Long> sizeMap = new HashMap<>();
        sizeMap.put('k', 1L << 10);
        sizeMap.put('M', 1L << 20);
        sizeMap.put('G', 1L << 30);
        sizeMap.put('T', 1L << 40);
        sizeMap.put('P', 1L << 50);
        sizeMap.put('E', 1L << 60);
        SIZE_MAP = unmodifiableMap(sizeMap);
    }

    public static long parse(String s) {
        final char lastChar;
        try {
            lastChar = s.charAt(s.length() - 1);
        } catch (IndexOutOfBoundsException ex) {
            throw new IllegalArgumentException("\"\" is not a valid size");
        }

        final char lastCharLower = Character.toLowerCase(lastChar);
        final char lastCharUpper = Character.toUpperCase(lastChar);
        final Long mul = SIZE_MAP.getOrDefault(lastCharUpper, SIZE_MAP.get(lastCharLower));
        if (mul != null)
            s = s.substring(0, s.length() - 1);

        final long v;
        try {
            v = Long.parseLong(s);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("\"" + s + "\" is not a number");
        }

        return v * (mul == null ? 1L : mul.longValue());
    }

    public static String render(long v) {
        return SIZE_MAP.entrySet().stream()
                .map(spec -> {
                    return Optional.of(v)
                            .filter(vv -> vv >= spec.getValue())
                            .filter(vv -> vv % spec.getValue() == 0)
                            .map(vv -> vv / spec.getValue())
                            .map(vv -> SimpleMapEntry.create(vv, spec.getKey()));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .min(Comparator.comparing(Map.Entry::getKey))
                .map(v_with_spec -> v_with_spec.getKey().toString() + v_with_spec.getValue().toString())
                .orElseGet(() -> Long.toString(v));
    }

    public static class BytesParserOptionHandler extends OptionHandler<Long> {
        public BytesParserOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Long> setter) {
            super(parser, option, setter);
        }

        @Override
        public int parseArguments(Parameters params) throws CmdLineException {
            final long value;
            try {
                value = parse(params.getParameter(0));
            } catch (IllegalArgumentException ex) {
                throw new CmdLineException(owner, "unable to parse size", ex);
            }

            setter.addValue(value);
            return 1;  // 1 argument consumed.
        }

        @Override
        public String getDefaultMetaVariable() {
            return "size";
        }

        @Override
        protected String print(Long v) {
            return render(v);
        }
    };
}
