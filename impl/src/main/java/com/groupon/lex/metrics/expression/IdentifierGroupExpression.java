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
package com.groupon.lex.metrics.expression;

import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Objects;

/**
 *
 * @author ariane
 */
public class IdentifierGroupExpression implements GroupExpression {
    private final String identifier_;

    public IdentifierGroupExpression(String identifier) {
        identifier_ = Objects.requireNonNull(identifier);
    }

    @Override
    public TimeSeriesValueSet getTSDelta(Context<?> ctx) {
        return ctx.getGroupFromIdentifier(identifier_)
                .orElse(TimeSeriesValueSet.EMPTY);
    }

    @Override
    public StringBuilder configString() {
        return ConfigSupport.maybeQuoteIdentifier(identifier_);
    }

    @Override
    public String toString() {
        return "IdentifierGroupExpression{" + identifier_ + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.identifier_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IdentifierGroupExpression other = (IdentifierGroupExpression) obj;
        if (!Objects.equals(this.identifier_, other.identifier_)) {
            return false;
        }
        return true;
    }
}
