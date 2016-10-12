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
package com.groupon.lex.metrics.history.v2.xdr;

import com.groupon.lex.metrics.lib.sequence.EmptyObjectSequence;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.EmptyTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Arrays;
import java.util.List;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.instanceOf;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class UtilTest {
    @Test
    public void segmentLength() {
        assertEquals(4, Util.segmentLength(0));
        assertEquals(8, Util.segmentLength(1));
        assertEquals(8, Util.segmentLength(2));
        assertEquals(8, Util.segmentLength(3));
        assertEquals(8, Util.segmentLength(4));
        assertEquals(32, Util.segmentLength(27));
        assertEquals(32, Util.segmentLength(28));
        assertEquals(36, Util.segmentLength(29));
        assertEquals(Long.MAX_VALUE - 3, Util.segmentLength(Long.MAX_VALUE - 8));
        assertEquals(Long.MAX_VALUE - 3, Util.segmentLength(Long.MAX_VALUE - 7));
    }

    @Test
    public void fixSequence() {
        List<TimeSeriesCollection> collection = Arrays.asList(
                new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC)),
                new EmptyTimeSeriesCollection(new DateTime(0, DateTimeZone.UTC)),
                new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC)));

        ObjectSequence<TimeSeriesCollection> fixed = Util.fixSequence(new ForwardSequence(0, collection.size()).map(collection::get, false, true, false));

        assertEquals(2, fixed.size());
        assertThat(fixed.toArray(new TimeSeriesCollection[0]),
                arrayContaining(
                        new EmptyTimeSeriesCollection(new DateTime(0, DateTimeZone.UTC)),
                        new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC))));
    }

    @Test
    public void fixSequenceNeedsNoFixing() {
        List<TimeSeriesCollection> collection = Arrays.asList(
                new EmptyTimeSeriesCollection(new DateTime(0, DateTimeZone.UTC)),
                new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC)));
        ObjectSequence<TimeSeriesCollection> inputSeq = new ForwardSequence(0, collection.size()).map(collection::get, true, true, true);

        ObjectSequence<TimeSeriesCollection> fixed = Util.fixSequence(inputSeq);

        assertSame(inputSeq, fixed);
    }

    @Test
    public void mergeSequences() {
        List<TimeSeriesCollection> collection1 = Arrays.asList(
                new EmptyTimeSeriesCollection(new DateTime(0, DateTimeZone.UTC)),
                new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC)));
        List<TimeSeriesCollection> collection2 = Arrays.asList(
                new EmptyTimeSeriesCollection(new DateTime(300, DateTimeZone.UTC)),
                new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC)));
        ObjectSequence<TimeSeriesCollection> inputSeq1 = new ForwardSequence(0, collection1.size()).map(collection1::get, true, true, true);
        ObjectSequence<TimeSeriesCollection> inputSeq2 = new ForwardSequence(0, collection2.size()).map(collection2::get, true, true, true);

        assertThat(Util.mergeSequences(), instanceOf(EmptyObjectSequence.class));
        assertSame(inputSeq1, Util.mergeSequences(inputSeq1));

        ObjectSequence<TimeSeriesCollection> fixed = Util.mergeSequences(inputSeq1, inputSeq2);
        assertThat(fixed.toArray(new TimeSeriesCollection[0]),
                arrayContaining(
                        new EmptyTimeSeriesCollection(new DateTime(0, DateTimeZone.UTC)),
                        new EmptyTimeSeriesCollection(new DateTime(300, DateTimeZone.UTC)),
                        new EmptyTimeSeriesCollection(new DateTime(600, DateTimeZone.UTC))));
    }
}
