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

import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.transformers.NameResolver;
import java.util.Objects;

/**
 *
 * @author ariane
 */
public class LiteralGroupExpression implements GroupExpression {
    private final NameResolver name_;

    public LiteralGroupExpression(NameResolver name) {
        name_ = Objects.requireNonNull(name);
    }

    public NameResolver getName() {
        return name_;
    }

    @Override
    public StringBuilder configString() {
        return name_.configString();
    }

    @Override
    public String toString() {
        return "LiteralGroupExpression{" + name_ + '}';
    }

    @Override
    public TimeSeriesValueSet getTSDelta(Context ctx) {
        return name_.apply(ctx)
                .map(path -> SimpleGroupPath.valueOf(path.getPath()))
                .map(name -> ctx.getTSData().getTSValue(name))
                .orElse(TimeSeriesValueSet.EMPTY);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + Objects.hashCode(this.name_);
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
        final LiteralGroupExpression other = (LiteralGroupExpression) obj;
        if (!Objects.equals(this.name_, other.name_)) {
            return false;
        }
        return true;
    }
}
