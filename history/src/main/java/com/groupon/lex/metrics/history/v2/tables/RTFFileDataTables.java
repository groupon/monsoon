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
package com.groupon.lex.metrics.history.v2.tables;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.v2.xdr.Util;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;

@Getter(AccessLevel.PRIVATE)
public final class RTFFileDataTables {
    private final file_data_tables input;
    private final ObjectSequence<RTFFileDataTablesBlock> blocks;
    private final SegmentReader<ObjectSequence<TimeSeriesCollection>> sequence;
    private final boolean sorted;
    private final boolean distinct;

    public RTFFileDataTables(@NonNull file_data_tables input, @NonNull SegmentReader.Factory<XdrAble> segmentFactory, boolean sorted, boolean distinct) {
        this.sorted = sorted;
        this.distinct = distinct;
        this.input = input;
        this.blocks = new ForwardSequence(0, input.blocks.length)
                .map(blockIdx -> new RTFFileDataTablesBlock(input.blocks[blockIdx], segmentFactory), true, true, true)
                .peek(RTFFileDataTablesBlock::validate)
                .share();
        this.sequence = SegmentReader.ofSupplier(this::buildSequence)
                .cache();
    }

    public void validate() {
    }

    private ObjectSequence<TimeSeriesCollection> buildSequence() {
        ObjectSequence[] seq = blocks.stream().map(block -> block.getTsdata()).toArray(ObjectSequence[]::new);
        if (!sorted || !distinct)
            return Util.mergeSequences(seq);
        return ObjectSequence.concat(seq, sorted, distinct);
    }

    public int size() {
        return sequence.decodeOrThrow().size();
    }

    public Iterator<TimeSeriesCollection> iterator() {
        return sequence.decodeOrThrow().iterator();
    }

    public Spliterator<TimeSeriesCollection> spliterator() {
        return sequence.decodeOrThrow().spliterator();
    }

    public Stream<TimeSeriesCollection> streamReversed() {
        return sequence.decodeOrThrow().reverse().stream();
    }

    public Stream<TimeSeriesCollection> stream() {
        return sequence.decodeOrThrow().stream();
    }

    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        ObjectSequence<TimeSeriesCollection> seq = sequence.decodeOrThrow();
        seq = seq.skip(seq.equalRange((tsc) -> tsc.getTimestamp().compareTo(begin)).getBegin());
        return seq.stream();
    }

    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        ObjectSequence<TimeSeriesCollection> seq = sequence.decodeOrThrow();
        seq = seq.skip(seq.equalRange((tsc) -> tsc.getTimestamp().compareTo(begin)).getBegin());
        seq = seq.limit(seq.equalRange((tsc -> tsc.getTimestamp().compareTo(end))).getEnd());
        return seq.stream();
    }

    public Set<SimpleGroupPath> getAllPaths() {
        return blocks.stream()
                .flatMap(block -> block.getAllPaths().stream())
                .collect(Collectors.toSet());
    }

    public Set<GroupName> getAllNames() {
        return blocks.stream()
                .flatMap(block -> block.getAllNames().stream())
                .collect(Collectors.toSet());
    }
}
