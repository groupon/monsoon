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
import com.groupon.lex.metrics.SimpleGroupPath;
import java.util.Collection;
import static java.util.Collections.EMPTY_SET;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;

/**
 * An empty time series collection.
 *
 * @author ariane
 */
@RequiredArgsConstructor
public class EmptyTimeSeriesCollection extends AbstractTimeSeriesCollection {
    @Getter
    private final DateTime timestamp;

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public Set<GroupName> getGroups(Predicate<? super GroupName> filter) {
        return EMPTY_SET;
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths(Predicate<? super SimpleGroupPath> filter) {
        return EMPTY_SET;
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return EMPTY_SET;
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return TimeSeriesValueSet.EMPTY;
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.empty();
    }
}
