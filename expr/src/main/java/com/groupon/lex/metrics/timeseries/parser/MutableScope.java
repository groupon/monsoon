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
package com.groupon.lex.metrics.timeseries.parser;

import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author ariane
 */
public class MutableScope implements Scope {
    private final Map<String, Type> identifiers_ = new HashMap<>();

    public MutableScope() {
    }

    public MutableScope(Scope parent) {
        this(parent.getIdentifiers());
    }

    public MutableScope(Scope parent, Map<String, Type> identifiers) {
        this(new HashMap<String, Type>() {
            {
                putAll(parent.getIdentifiers());
                putAll(identifiers);
            }
        });
    }

    public MutableScope(Map<String, Type> identifiers) {
        identifiers_.putAll(identifiers);
    }

    public void put(String identifier, Type type) {
        identifiers_.put(identifier, type);
    }

    public void putAll(Map<String, Type> identifiers) {
        identifiers_.putAll(identifiers);
    }

    public void remove(String identifier) {
        identifiers_.remove(identifier);
    }

    @Override
    public Map<String, Type> getIdentifiers() {
        return unmodifiableMap(identifiers_);
    }
}
