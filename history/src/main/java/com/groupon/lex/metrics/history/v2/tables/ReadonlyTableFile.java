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
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.path_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.strval_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.v2.xdr.tag_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.Const;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.SegmentBufferSupplier;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import static gnu.trove.TCollections.unmodifiableMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.joda.time.DateTime;

public class ReadonlyTableFile {
    private static final short FILE_VERSION = 3;  // Only file version that uses Table format.
    private final SegmentReader<FileDataTables> body;
    @Getter
    private final DateTime begin, end;
    @Getter
    private final long fileSize;
    private final SegmentReader.Factory<XdrAble> segmentFactory;

    public ReadonlyTableFile(GCCloseable<FileChannel> file) throws IOException, OncRpcException {
        fileSize = file.get().size();

        XdrDecodingStream reader = new XdrBufferDecodingStream(new SegmentBufferSupplier(file, 0, fileSize));
        final int version = validateHeaderOrThrow(reader);
        if (Const.version_major(version) != FILE_VERSION)
            throw new IllegalArgumentException("TableFile is version 3 only");

        tsfile_header hdr = new tsfile_header(reader);
        if ((hdr.flags & header_flags.KIND_MASK) != header_flags.KIND_TABLES)
            throw new IllegalArgumentException("Not a file in table encoding");
        final boolean compressed = ((hdr.flags & header_flags.GZIP) == header_flags.GZIP);

        if ((hdr.flags & header_flags.DUP_TS) == header_flags.DUP_TS)
            throw new IllegalArgumentException("Bad TableFile: marked as containing duplicate timestamps");
        if ((hdr.flags & header_flags.SORTED) != header_flags.SORTED)
            throw new IllegalArgumentException("Bad TableFile: marked as unsorted");

        segmentFactory = new FileChannelSegmentReader.Factory(file, compressed);
        begin = FromXdr.timestamp(hdr.first);
        end = FromXdr.timestamp(hdr.last);
        final FilePos bodyPos = new FilePos(hdr.fdt);

        body = segmentFactory.get(file_data_tables::new, bodyPos)
                .map(fdt -> new FileDataTables(fdt, segmentFactory))
                .cache();
    }

    private static final class FileDataTables {
        private final SegmentReader.Factory<XdrAble> segmentFactory;
        @Getter
        private final Dictionary dictionary;
        private final Map<SimpleGroupPath, Map<Tags, SegmentReader<group_table>>> groups;

        public FileDataTables(file_data_tables input, SegmentReader.Factory segmentFactory) {
            this.segmentFactory = segmentFactory;
            this.dictionary = new Dictionary(input.dictionary);
            this.groups = unmodifiableMap(Arrays.stream(input.tables_data.value)
                    .collect(Collectors.toMap(
                            tg -> SimpleGroupPath.valueOf(getDictionary().getPath(tg.group_ref)),
                            tg -> unmodifiableMap(innerMap(tg))))
            );
        }

        private Map<Tags, SegmentReader<group_table>> innerMap(tables_group tg) {
            return Arrays.stream(tg.tag_tbl)
                    .collect(Collectors.toMap(
                            tt -> getDictionary().getTags(tt.tag_ref),
                            tt -> segmentFromFilePos(new FilePos(tt.pos))
                    ));
        }

        private SegmentReader<group_table> segmentFromFilePos(FilePos fp) {
            return segmentFactory.get(group_table::new, fp)
                    // XXX .map(GroupTable::new)
                    .share();
        }

        public Set<SimpleGroupPath> getAllPaths() { return groups.keySet(); }
        public Set<GroupName> getAllNames() {
            return groups.entrySet().stream()
                    .flatMap(pathEntry -> {
                        return pathEntry.getValue().keySet().stream()
                                .map(tags -> GroupName.valueOf(pathEntry.getKey(), tags));
                    })
                    .collect(Collectors.toSet());
        }

        public SegmentReader<group_table> getGroupTable(GroupName group) {
            return groups.getOrDefault(group.getPath(), emptyMap())
                    .get(group.getTags());
        }
    }

    private static final class Dictionary {
        private final TIntObjectMap<String> stringTable;
        private final TIntObjectMap<List<String>> pathTable;
        private final TIntObjectMap<Tags> tagsTable;

        public Dictionary(dictionary_delta input) {
            stringTable = unmodifiableMap(decode(input.sdd));
            pathTable = unmodifiableMap(decode(input.pdd, stringTable));
            tagsTable = unmodifiableMap(decode(input.tdd, stringTable));
        }

        private static TIntObjectMap<String> decode(strval_dictionary_delta sdd) {
            final TIntObjectMap<String> stringTable = new TIntObjectHashMap<>(sdd.values.length, 1f);
            for (int i = 0; i < sdd.values.length; ++i)
                stringTable.put(sdd.offset + i, sdd.values[i].value);
            return stringTable;
        }

        private static TIntObjectMap<List<String>> decode(path_dictionary_delta pdd, TIntObjectMap<String> stringTable) {
            final TIntObjectMap<List<String>> pathTable = new TIntObjectHashMap<>(pdd.values.length, 1f);
            for (int i = 0; i < pdd.values.length; ++i) {
                final List<String> value = unmodifiableList(Arrays.asList(Arrays.stream(pdd.values[i].value)
                        .mapToObj(str_ref -> requireNonNull(stringTable.get(str_ref), "Invalid string reference"))
                        .toArray(String[]::new)));
                pathTable.put(pdd.offset + i, value);
            }
            return pathTable;
        }

        private static TIntObjectMap<Tags> decode(tag_dictionary_delta tdd, TIntObjectMap<String> stringTable) {
            final TIntObjectMap<Tags> tagsTable = new TIntObjectHashMap<>(tdd.values.length, 1f);
            for (int i = 0; i < tdd.values.length; ++i) {
                final tags current = tdd.values[i];
                final List<Map.Entry<String, MetricValue>> tagEntries = new ArrayList<>(current.str_ref.length);

                for (int tagIdx = 0; tagIdx < current.str_ref.length; ++i) {
                    final String key = requireNonNull(stringTable.get(current.str_ref[tagIdx]), "Invalid string reference");
                    final MetricValue val = FromXdr.metricValue(current.value[tagIdx], (sref) -> requireNonNull(stringTable.get(sref), "Invalid string reference"));
                    tagEntries.add(SimpleMapEntry.create(key, val));
                }
                tagsTable.put(tdd.offset + i, Tags.valueOf(tagEntries.stream()));
            }
            return tagsTable;
        }

        public String getString(int ref) {
            return requireNonNull(stringTable.get(ref), "Invalid string reference");
        }

        public Tags getTags(int ref) {
            return requireNonNull(tagsTable.get(ref), "Invalid tags reference");
        }

        public List<String> getPath(int ref) {
            return requireNonNull(pathTable.get(ref), "Invalid path reference");
        }
    }
}
