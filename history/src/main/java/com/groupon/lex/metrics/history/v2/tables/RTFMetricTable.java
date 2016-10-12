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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.bitset;
import com.groupon.lex.metrics.history.v2.xdr.histogram;
import com.groupon.lex.metrics.history.v2.xdr.metric_table;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.v2.xdr.mt_16bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_32bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_64bit;
import com.groupon.lex.metrics.history.v2.xdr.mt_bool;
import com.groupon.lex.metrics.history.v2.xdr.mt_dbl;
import com.groupon.lex.metrics.history.v2.xdr.mt_hist;
import com.groupon.lex.metrics.history.v2.xdr.mt_other;
import com.groupon.lex.metrics.history.v2.xdr.mt_str;
import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.history.xdr.support.IntegrityException;

public class RTFMetricTable {
    private final MtBoolValues m_bool;
    private final Mt16BitValues m_16bit;
    private final Mt32BitValues m_32bit;
    private final Mt64BitValues m_64bit;
    private final MtDblValues m_dbl;
    private final MtStrValues m_str;
    private final MtHistValues m_hist;
    private final MtEmptyValues m_empty;
    private final MtOtherValues m_other;

    public RTFMetricTable(metric_table input, DictionaryDelta dictionary) {
        m_bool = new MtBoolValues(input.metrics_bool);
        m_16bit = new Mt16BitValues(input.metrics_16bit);
        m_32bit = new Mt32BitValues(input.metrics_32bit);
        m_64bit = new Mt64BitValues(input.metrics_64bit);
        m_dbl = new MtDblValues(input.metrics_dbl);
        m_str = new MtStrValues(input.metrics_str, dictionary);
        m_hist = new MtHistValues(input.metrics_hist);
        m_empty = new MtEmptyValues(input.metrics_hist);
        m_other = new MtOtherValues(input.metrics_other, dictionary);
    }

    public boolean contains(int index) {
        return m_bool.contains(index) ||
                m_16bit.contains(index) ||
                m_32bit.contains(index) ||
                m_64bit.contains(index) ||
                m_dbl.contains(index) ||
                m_str.contains(index) ||
                m_hist.contains(index) ||
                m_empty.contains(index) ||
                m_other.contains(index);
    }

    public MetricValue get(int index) {
        MetricValue mv;

        mv = m_bool.get(index);
        if (mv != null) return mv;

        mv = m_16bit.get(index);
        if (mv != null) return mv;

        mv = m_32bit.get(index);
        if (mv != null) return mv;

        mv = m_64bit.get(index);
        if (mv != null) return mv;

        mv = m_dbl.get(index);
        if (mv != null) return mv;

        mv = m_str.get(index);
        if (mv != null) return mv;

        mv = m_hist.get(index);
        if (mv != null) return mv;

        mv = m_empty.get(index);
        if (mv != null) return mv;

        mv = m_other.get(index);
        if (mv != null) return mv;

        throw new DecodingException("requested metric has not value");
    }

    public void validate() {
        m_bool.validate();
        m_16bit.validate();
        m_32bit.validate();
        m_64bit.validate();
        m_dbl.validate();
        m_str.validate();
        m_hist.validate();
        m_empty.validate();
        m_other.validate();
    }

    private static abstract class MtTable {
        private final int[] map;

        protected MtTable(bitset input) {
            boolean[] presence = FromXdr.bitset(input);
            int len = 0;
            for (int i = 0; i < presence.length; ++i)
                if (presence[i]) ++len;

            int buildIdx = 0;
            map = new int[presence.length];
            for (int i = 0; i < presence.length; ++i) {
                if (presence[i])
                    map[i] = buildIdx++;
                else
                    map[i] = -1;
            }
            assert(buildIdx == len);
        }

        public final boolean contains(int index) {
            return index >= 0 && index < map.length && map[index] >= 0;
        }

        public final MetricValue get(int index) {
            if (index < 0 || index >= map.length) return null;
            final int innerIdx = map[index];
            if (innerIdx < 0) return null;
            return doGet(innerIdx);
        }

        public void validate() {
            validateInner();
        }

        protected void validateInner() {
            int expectedCount = 0;
            for (int i = 0; i < map.length; ++i)
                if (map[i] >= 0) ++expectedCount;

            if (innerSize() != expectedCount)
                throw new IntegrityException("mismatch in metric table encoding");
        }

        protected abstract MetricValue doGet(int innerIdx);
        protected abstract int innerSize();
    }

    private static class MtBoolValues extends MtTable {
        private final boolean values[];

        public MtBoolValues(mt_bool input) {
            super(input.presence);
            values = FromXdr.bitset(input.values);
        }

        @Override
        protected MetricValue doGet(int idx) {
            return MetricValue.fromBoolean(values[idx]);
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class Mt16BitValues extends MtTable {
        private final short values[];

        public Mt16BitValues(mt_16bit input) {
            super(input.presence);
            values = input.values;
        }

        @Override
        protected MetricValue doGet(int idx) {
            return MetricValue.fromIntValue(values[idx]);
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class Mt32BitValues extends MtTable {
        private final int values[];

        public Mt32BitValues(mt_32bit input) {
            super(input.presence);
            values = input.values;
        }

        @Override
        protected MetricValue doGet(int idx) {
            return MetricValue.fromIntValue(values[idx]);
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class Mt64BitValues extends MtTable {
        private final long values[];

        public Mt64BitValues(mt_64bit input) {
            super(input.presence);
            values = input.values;
        }

        @Override
        protected MetricValue doGet(int idx) {
            return MetricValue.fromIntValue(values[idx]);
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class MtDblValues extends MtTable {
        private final double values[];

        public MtDblValues(mt_dbl input) {
            super(input.presence);
            values = input.values;
        }

        @Override
        protected MetricValue doGet(int idx) {
            return MetricValue.fromDblValue(values[idx]);
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class MtStrValues extends MtTable {
        private final DictionaryDelta dictionary;
        private final int values[];

        public MtStrValues(mt_str input, DictionaryDelta dictionary) {
            super(input.presence);
            this.dictionary = dictionary;
            values = input.values;
        }

        @Override
        protected MetricValue doGet(int idx) {
            return MetricValue.fromStrValue(dictionary.getString(values[idx]));
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class MtHistValues extends MtTable {
        private final histogram values[];

        public MtHistValues(mt_hist input) {
            super(input.presence);
            values = input.values;
        }

        @Override
        public MetricValue doGet(int idx) {
            return MetricValue.fromHistValue(FromXdr.histogram(values[idx]));
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }

    private static class MtEmptyValues extends MtTable {
        public MtEmptyValues(mt_hist input) {
            super(input.presence);
        }

        @Override
        public MetricValue doGet(int idx) {
            return MetricValue.EMPTY;
        }

        @Override
        public void validateInner() {}

        @Override
        protected int innerSize() {
            return 0;
        }
    }

    private static class MtOtherValues extends MtTable {
        private final DictionaryDelta dictionary;
        private final metric_value values[];

        public MtOtherValues(mt_other input, DictionaryDelta dictionary) {
            super(input.presence);
            this.dictionary = dictionary;
            values = input.values;
        }

        @Override
        public MetricValue doGet(int idx) {
            return FromXdr.metricValue(values[idx], dictionary::getString);
        }

        @Override
        protected int innerSize() {
            return values.length;
        }
    }
}
