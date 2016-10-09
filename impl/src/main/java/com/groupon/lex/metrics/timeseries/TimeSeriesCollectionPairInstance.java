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

import com.groupon.lex.metrics.MutableTimeSeriesCollectionPair;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public class TimeSeriesCollectionPairInstance extends AbstractTSCPair implements MutableTimeSeriesCollectionPair {
    private final MutableTimeSeriesCollection current_;

    public TimeSeriesCollectionPairInstance(DateTime now) {
        current_ = new MutableTimeSeriesCollection(now);
    }

    @Override
    public MutableTimeSeriesCollection getCurrentCollection() {
        return current_;
    }

    public TimeSeriesCollectionPairInstance startNewCycle(DateTime timestamp, ExpressionLookBack lookback) {
        update(
                new SimpleTimeSeriesCollection(current_.getTimestamp(), current_.getTSValues()),
                lookback,
                () -> current_.clear(timestamp));
        return this;
    }

    @Override
    public String toString() {
        return "TimeSeriesCollectionPairInstance{current_=" + current_ + ", " + super.toString() + '}';
    }
}
