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
package com.groupon.lex.metrics.collector.httpget;

import com.groupon.lex.metrics.lib.StringTemplate;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import lombok.Getter;

/**
 *
 * @author ariane
 */
@Getter
public class UrlPattern {
    private final StringTemplate urlTemplate;
    private final NameBoundResolver templateArgs;

    public UrlPattern(@NonNull StringTemplate urlTemplate, @NonNull NameBoundResolver templateArgs) {
        this.urlTemplate = urlTemplate;
        this.templateArgs = templateArgs;

        if (!this.templateArgs.getKeys().collect(Collectors.toSet()).containsAll(this.urlTemplate.getArguments()))
            throw new IllegalArgumentException("Not all parameters are fulfilled.");
    }

    public UrlPattern(String urlTemplate, NameBoundResolver templateArgs) {
        this(StringTemplate.fromString(urlTemplate), templateArgs);
    }

    private Map.Entry<GroupName, String> apply_args_(NamedResolverMap args) {
        final Map<Any2<Integer, String>, String> stringArgs = args.getStringMap();
        return SimpleMapEntry.create(args.getGroupName(), urlTemplate.apply(stringArgs));
    }

    public Stream<Map.Entry<GroupName, String>> getUrls() throws Exception {
        return templateArgs.resolve()
                .map(this::apply_args_);
    }

    @Override
    public String toString() {
        return urlTemplate.toString();
    }
}
