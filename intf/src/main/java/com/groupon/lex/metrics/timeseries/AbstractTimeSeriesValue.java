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
package com.groupon.lex.metrics.timeseries;

import java.util.Objects;

/**
 * Provides hashCode, equality functions.
 * @author ariane
 */
public abstract class AbstractTimeSeriesValue implements TimeSeriesValue {
    @Override
    public int hashCode() {
        return hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TimeSeriesValue)) {
            return false;
        }
        final TimeSeriesValue other = (TimeSeriesValue) obj;
        return equals(this, other);
    }

    @Override
    public String toString() {
        return "TimeSeriesValue{" + "timestamp=" + getTimestamp() + ", group=" + getGroup() + ", metrics=" + getMetrics() + '}';
    }

    @Override
    public abstract TimeSeriesValue clone();

    public static int hashCode(TimeSeriesValue v) {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(v.getGroup());
        return hash;
    }

    public static boolean equals(TimeSeriesValue a, TimeSeriesValue b) {
        if (!Objects.equals(a.getTimestamp(), b.getTimestamp())) {
            return false;
        }
        if (!Objects.equals(a.getGroup(), b.getGroup())) {
            return false;
        }
        if (!Objects.equals(a.getMetrics(), b.getMetrics())) {
            return false;
        }
        return true;
    }
}
