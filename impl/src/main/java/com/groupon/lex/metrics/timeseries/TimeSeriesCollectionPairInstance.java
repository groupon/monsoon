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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MutableTimeSeriesCollectionPair;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.CollectHistory;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
@ToString
public final class TimeSeriesCollectionPairInstance implements MutableTimeSeriesCollectionPair {
    @Getter
    private final MutableTimeSeriesCollection currentCollection;
    private Impl impl = new InMemoryImplementation();

    public TimeSeriesCollectionPairInstance(DateTime now) {
        currentCollection = new MutableTimeSeriesCollection(now);
    }

    public TimeSeriesCollectionPairInstance startNewCycle(DateTime timestamp, ExpressionLookBack lookback) {
        impl.startNewCycle(lookback, () -> currentCollection.clear(timestamp));
        return this;
    }

    public void initWithHistoricalData(CollectHistory history, ExpressionLookBack lookback) {
        impl = new HistoryBackedImplementation(history, lookback);
    }

    @Override
    public TimeSeriesCollection getPreviousCollection() {
        return impl.getPreviousCollection();
    }

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(int n) {
        return impl.getPreviousCollection(n);
    }

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(Duration duration) {
        return impl.getPreviousCollection(duration);
    }

    @Override
    public TimeSeriesCollectionPair getPreviousCollectionPair(int n) {
        return impl.getPreviousCollectionPair(n);
    }

    @Override
    public TimeSeriesCollectionPair getPreviousCollectionPair(Duration duration) {
        return impl.getPreviousCollectionPair(duration);
    }

    @Override
    public List<TimeSeriesCollectionPair> getCollectionPairsSince(Duration duration) {
        return impl.getCollectionPairsSince(duration);
    }

    @Override
    public TimeSeriesCollection getPreviousCollectionAt(DateTime ts) {
        return impl.getPreviousCollectionAt(ts);
    }

    @Override
    public TimeSeriesCollection getPreviousCollectionAt(Duration duration) {
        return impl.getPreviousCollectionAt(duration);
    }

    @Override
    public TimeSeriesCollectionPair getPreviousCollectionPairAt(DateTime ts) {
        return impl.getPreviousCollectionPairAt(ts);
    }

    @Override
    public TimeSeriesCollectionPair getPreviousCollectionPairAt(Duration duration) {
        return impl.getPreviousCollectionPairAt(duration);
    }

    @Override
    public Duration getCollectionInterval() {
        return impl.getCollectionInterval();
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return impl.getTSValue(name);
    }

    @Override
    public Optional<TimeSeriesValueSet> getTSDeltaByName(GroupName name) {
        return impl.getTSDeltaByName(name);
    }

    @Override
    public int size() {
        return impl.size();
    }

    private static interface Impl extends TimeSeriesCollectionPair {
        public void startNewCycle(ExpressionLookBack lookback, Runnable doBeforeValidation);
    }

    @ToString(callSuper = true)
    private class InMemoryImplementation extends AbstractTSCPair implements Impl {
        @Override
        public TimeSeriesCollection getCurrentCollection() {
            return TimeSeriesCollectionPairInstance.this.getCurrentCollection();
        }

        @Override
        public void startNewCycle(ExpressionLookBack lookback, Runnable doBeforeValidation) {
            update(new SimpleTimeSeriesCollection(getCurrentCollection().getTimestamp(), getCurrentCollection().getTSValues()),
                    lookback,
                    doBeforeValidation);
        }
    }

    @ToString(callSuper = true)
    private class HistoryBackedImplementation extends ChainingTSCPair implements Impl {
        public HistoryBackedImplementation(CollectHistory history, ExpressionLookBack lookback) {
            super(history, lookback);
        }

        @Override
        public TimeSeriesCollection getCurrentCollection() {
            return TimeSeriesCollectionPairInstance.this.getCurrentCollection();
        }

        @Override
        public void startNewCycle(ExpressionLookBack lookback, Runnable doBeforeValidation) {
            update(getCurrentCollection(), lookback, doBeforeValidation);
        }
    }
}
