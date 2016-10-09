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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.function.Consumer;

/**
 * A wrapper, that returns the context from the previous collection.
 * @author ariane
 */
public class PreviousContextWrapper implements Context<TimeSeriesCollectionPair> {
    private static final TimeSeriesCollection empty_tsc_ = new MutableTimeSeriesCollection();
    private final Context ctx_;
    private final TimeSeriesCollectionPair tsdata_;

    public PreviousContextWrapper(Context ctx, TimeSeriesCollectionPair tsdata) {
        ctx_ = requireNonNull(ctx);
        tsdata_ = requireNonNull(tsdata);
    }

    @Override
    public Map<String, ContextIdentifier> getAllIdentifiers() { return ctx_.getAllIdentifiers(); };
    @Override
    public Consumer<Alert> getAlertManager() { return ctx_.getAlertManager(); }
    @Override
    public TimeSeriesCollectionPair getTSData() { return tsdata_; }
}
