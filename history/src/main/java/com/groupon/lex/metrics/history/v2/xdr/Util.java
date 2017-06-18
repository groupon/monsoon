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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.support.ByteCountingXdrEncodingStream.xdrSize;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import static com.groupon.lex.metrics.history.xdr.support.reader.Crc32Reader.CRC_LEN;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.joda.time.DateTime;

public class Util {
    /**
     * Describe the length of the version 3 header.
     */
    public static final int HDR_3_LEN;
    /**
     * Number of bytes used by mime header + version 3 header + crc.
     */
    public static final int ALL_HDR_CRC_LEN;

    static {
        {
            tsfile_header sample = new tsfile_header();
            sample.fdt = ToXdr.filePos(new FilePos(0, 0));
            sample.file_size = 0;
            sample.first = ToXdr.timestamp(0);
            sample.last = ToXdr.timestamp(0);
            sample.flags = 0;
            sample.reserved = 0;
            HDR_3_LEN = (int) xdrSize(sample);
        }

        ALL_HDR_CRC_LEN = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
    }

    public static long segmentLength(long dataLength) {
        return ((dataLength + 3) & ~3l) + CRC_LEN;
    }

    /**
     * Fix a sequence to contain only distinct, sorted elements.
     */
    public static ObjectSequence<TimeSeriesCollection> fixSequence(ObjectSequence<TimeSeriesCollection> seq) {
        seq = seq.sort();  // Does nothing if already sorted.

        if (!seq.isDistinct() && !seq.isEmpty()) {
            List<ObjectSequence<TimeSeriesCollection>> toConcat = new ArrayList<>();
            int lastGoodIdx = 0;
            int curIdx = 0;

            while (curIdx < seq.size()) {
                final TimeSeriesCollection curElem = seq.get(curIdx);

                // Find first item with different timestamp.
                int nextIdx = curIdx + 1;
                while (nextIdx < seq.size()
                        && curElem.getTimestamp().equals(seq.get(nextIdx).getTimestamp())) {
                    ++nextIdx;
                }

                // If more than 1 adjecent element share timestamp, merge them together.
                if (curIdx + 1 < nextIdx) {
                    toConcat.add(seq.limit(curIdx).skip(lastGoodIdx));

                    TimeSeriesCollection replacement = new LazyMergedTSC(seq.limit(nextIdx).skip(curIdx));
                    toConcat.add(ObjectSequence.of(true, true, true, replacement));
                    lastGoodIdx = curIdx = nextIdx;
                } else {
                    // Advance curIdx.
                    ++curIdx;
                }
            }

            if (lastGoodIdx < curIdx)
                toConcat.add(seq.skip(lastGoodIdx));
            seq = ObjectSequence.concat(toConcat, true, true);
        }

        return seq;
    }

    /**
     * Given zero or more sequences, that are all sorted and distinct, merge
     * them together.
     *
     * @param tsSeq Zero or more sequences to process.
     * @return A merged sequence.
     */
    public static ObjectSequence<TimeSeriesCollection> mergeSequences(ObjectSequence<TimeSeriesCollection>... tsSeq) {
        if (tsSeq.length == 0)
            return ObjectSequence.empty();
        if (tsSeq.length == 1)
            return tsSeq[0];

        // Sort sequences and remove any that are empty.
        final Queue<ObjectSequence<TimeSeriesCollection>> seq = new PriorityQueue<>(Comparator.comparing(ObjectSequence::first));
        seq.addAll(Arrays.stream(tsSeq)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList()));
        // It's possible the filtering reduced the number of elements to 0 or 1...
        if (seq.isEmpty())
            return ObjectSequence.empty();
        if (seq.size() == 1)
            return seq.element();

        final List<ObjectSequence<TimeSeriesCollection>> output = new ArrayList<>(tsSeq.length);
        while (!seq.isEmpty()) {
            final ObjectSequence<TimeSeriesCollection> head = seq.remove();
            if (seq.isEmpty()) {
                output.add(head);
                continue;
            }

            if (head.last().compareTo(seq.element().first()) < 0) {
                output.add(head);
                continue;
            }

            final List<ObjectSequence<TimeSeriesCollection>> toMerge = new ArrayList<>();

            // Find the intersecting range.
            final int headProblemStart = head.equalRange(tsc -> tsc.compareTo(seq.element().first())).getBegin();
            output.add(head.limit(headProblemStart));  // Add non-intersecting range to output.
            toMerge.add(head.skip(headProblemStart));  // Add problematic area to 'toMerge' collection.

            // Find all remaining intersecting ranges and add them to 'toMerge', replacing them with their non-intersecting ranges.
            final TimeSeriesCollection headLast = head.last();
            while (!seq.isEmpty() && seq.element().first().compareTo(headLast) <= 0) {
                final ObjectSequence<TimeSeriesCollection> succ = seq.remove();
                TimeSeriesCollection succFirst = succ.first();
                System.err.println("succ.first: " + succ.first() + ", head.last: " + headLast);

                // Add intersecting range of succ to 'toMerge'.
                final int succProblemEnd = succ.equalRange(tsc -> tsc.compareTo(headLast)).getEnd();
                assert succProblemEnd > 0;
                toMerge.add(succ.limit(succProblemEnd));

                // Add non-intersecting range of succ back to 'seq'.
                ObjectSequence<TimeSeriesCollection> succNoProblem = succ.skip(succProblemEnd);
                if (!succNoProblem.isEmpty())
                    seq.add(succNoProblem);
            }

            assert (toMerge.size() > 1);
            output.add(fixSequence(ObjectSequence.concat(toMerge, false, false)));
        }

        return ObjectSequence.concat(output, true, true);
    }

    private static class LazyMergedTSC extends AbstractTimeSeriesCollection {
        private final ObjectSequence<TimeSeriesCollection> underlying;

        public LazyMergedTSC(@NonNull ObjectSequence<TimeSeriesCollection> underlying) {
            this.underlying = underlying;
            if (underlying.isEmpty())
                throw new IllegalArgumentException("cannot merge 0 TimeSeriesCollections");
        }

        @Override
        public DateTime getTimestamp() {
            return underlying.first().getTimestamp();
        }

        @Override
        public boolean isEmpty() {
            return underlying.stream()
                    .allMatch(TimeSeriesCollection::isEmpty);
        }

        @Override
        public Set<GroupName> getGroups(Predicate<? super GroupName> filter) {
            return underlying.stream()
                    .flatMap(tsc -> tsc.getGroups(filter).stream())
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<SimpleGroupPath> getGroupPaths(Predicate<? super SimpleGroupPath> filter) {
            return underlying.stream()
                    .flatMap(tsc -> tsc.getGroupPaths(filter).stream())
                    .collect(Collectors.toSet());
        }

        @Override
        public Collection<TimeSeriesValue> getTSValues() {
            return underlying.stream()
                    .flatMap(tsc -> tsc.getTSValues().stream())
                    .collect(Collectors.toMap(TimeSeriesValue::getGroup, Function.identity(), (a, b) -> a))
                    .values();
        }

        @Override
        public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
            return new TimeSeriesValueSet(underlying.stream()
                    .flatMap(tsc -> tsc.getTSValue(name).stream())
                    .collect(Collectors.toMap(TimeSeriesValue::getGroup, Function.identity(), (a, b) -> a))
                    .values());
        }

        @Override
        public Optional<TimeSeriesValue> get(GroupName name) {
            return underlying.stream()
                    .map(tsc -> tsc.get(name))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();
        }

        @Override
        public TimeSeriesValueSet get(Predicate<? super SimpleGroupPath> pathFilter, Predicate<? super GroupName> groupFilter) {
            return new TimeSeriesValueSet(underlying.stream()
                    .map(tsc -> tsc.get(pathFilter, groupFilter))
                    .flatMap(tsvSet -> tsvSet.stream())
                    .collect(Collectors.toMap(TimeSeriesValue::getGroup, Function.identity(), (x, y) -> x)) // Conflict resolution: use first occurance.
                    .values());
        }
    }
}
