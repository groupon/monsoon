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
package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.timeseries.BackRefTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Small iterator wrapper that merges subsequent items.
 * @author ariane
 */
public class FileIterator implements Iterator<TimeSeriesCollection> {
    private final Iterator<TimeSeriesCollection> iter;
    private TimeSeriesCollection next;

    public FileIterator(Iterator<TimeSeriesCollection> iter) {
        this.iter = iter;

        if (this.iter.hasNext())
            next = this.iter.next();
        else
            next = null;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public TimeSeriesCollection next() {
        TimeSeriesCollection result = next;
        if (iter.hasNext()) {
            next = iter.next();
        } else {
            next = null;
            return result;
        }

        Collection<TimeSeriesCollection> tsMerge = new ArrayList<>();
        while (next != null && next.getTimestamp().equals(result.getTimestamp())) {
            tsMerge.add(next);
            if (iter.hasNext())
                next = iter.next();
            else
                next = null;
        }
        result = new BackRefTimeSeriesCollection(
                result.getTimestamp(),
                Stream.concat(
                        result.getTSValues().stream(),
                        tsMerge.stream().flatMap(c -> c.getTSValues().stream())));
        return result;
    }
}
