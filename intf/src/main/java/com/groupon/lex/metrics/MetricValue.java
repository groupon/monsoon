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
package com.groupon.lex.metrics;

import java.io.Serializable;
import static java.util.Objects.requireNonNull;
import java.util.Optional;

/**
 * Models a single metric value.
 *
 * A metric value can be:
 * - null (no value present)
 * - an integer type
 * - a floating point type
 * - a string
 * - a histogram
 *
 * Because this class needs to be serializable using the JMX MXBean spec, it
 * encodes each type separately, using null values to fill in absent types.
 *
 * MetricValue is an immutable class.
 * @author ariane
 */
public abstract class MetricValue implements Comparable<MetricValue>, Serializable {
    public static final MetricValue TRUE = new BooleanMetricValue(true);
    public static final MetricValue FALSE = new BooleanMetricValue(false);
    public static final Optional<MetricValue> OPTIONAL_TRUE = Optional.of(TRUE);
    public static final Optional<MetricValue> OPTIONAL_FALSE = Optional.of(FALSE);

    public static MetricValue fromBoolean(boolean b) { return (b ? TRUE : FALSE); }
    public static MetricValue fromIntValue(long int_value) { return new IntMetricValue(int_value); }
    public static MetricValue fromDblValue(double flt_value) { return new DblMetricValue(flt_value); }
    public static MetricValue fromStrValue(String str_value) { return new StringMetricValue(str_value); }
    public static MetricValue fromHistValue(Histogram hist_value) { return new HistogramMetricValue(hist_value); }

    /** Only allow derived types in this class to access the MetricValue interface. */
    private MetricValue() {}

    /**
     * Construct a metric value from a Number implementation.
     * @param number A floating point or integral type.
     * @return a MetricValue holding the specified number.
     * @throws IllegalArgumentException if the derived type of Number is not recognized.
     */
    public static MetricValue fromNumberValue(Number number) {
        if (number == null) {
            return EMPTY;
        } else if (number instanceof Float || number instanceof Double) {
            return fromDblValue(number.doubleValue());
        } else if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
            return fromIntValue(number.longValue());
        } else {
            throw new IllegalArgumentException("Unrecognized number type: " + number.getClass());
        }
    }

    /** The empty metric value. */
    public static MetricValue EMPTY = new EmptyMetricValue();

    @Override
    public abstract int compareTo(MetricValue o);

    /**
     * Numeric getter, returns the number value contained in this MetricValue.
     *
     * Boolean values are treated as integral values, with 1 == true, 0 == false.
     * @return An optional, holding the numeric value of this MetricValue.
     *         If the MetricValue holds no numeric value, an empty Optional is returned.
     */
    public abstract Optional<Number> value();

    /**
     * Boolean getter, returns boolean corresponding to boolean or numeric MetricValue.
     *
     * Numeric values are treated as:
     * - true if not zero
     * - false if zero
     * @return An optional, holding the boolean value of this MetricValue.
     *         If the MetricValue holds no boolean coercible value, an empty Optional is returned.
     */
    public abstract Optional<Boolean> asBool();

    /**
     * String getter, returns string corresponding to boolean, numeric or string MetricValue.
     *
     * @return An optional, holding the string value of this MetricValue.
     *         If the MetricValue holds no value, an empty Optional is returned.
     */
    public abstract Optional<String> asString();

    /**
     * String getter, returns the string value contained in this MetricValue.
     * @return An optional, holding the string value of this MetricValue.
     *         If the MetricValue holds no string value, an empty Optional is returned.
     */
    public Optional<String> stringValue() { return Optional.empty(); }

    /**
     * Histogram getter, returns the histogram value contained in this MetricValue.
     * @return An optional, holding the histogram value of this MetricValue.
     *         If the MetricValue holds no histogram, en empty Optional is returned.
     */
    public Optional<Histogram> histogram() { return Optional.empty(); }

    /**
     * Raw interface: get value of boolean type.
     * @return The boolean value, or null if this holds no boolean value.
     */
    public Boolean getBoolValue() { return null; }

    /**
     * Raw interface: get value of integer type.
     * @return The integer value, or null if this holds no integer value.
     */
    public Long getIntValue() { return null; }

    /**
     * Raw interface: get value of floating point type.
     * @return The floating point value, or null if this holds no floating point value.
     */
    public Double getFltValue() { return null; }

    /**
     * Raw interface: get value of string.
     * @return The string value, or null if this holds no string value.
     */
    public String getStrValue() { return null; }

    /**
     * Raw interface: get value of histogram.
     * @return The histogram value, or null if this holds no histogram value.
     */
    public Histogram getHistValue() { return null; }

    /**
     * A string representation of the MetricValue, that is valid in the configuration.
     * Returns null if the MetricValue is empty.
     */
    public abstract String configString();

    /** Returns the config string, or "(none)" if this is an empty metric value. */
    @Override
    public String toString() {
        final String cfg_s = configString();
        return cfg_s == null ? "(none)" : cfg_s;
    }

    @Override
    public abstract boolean equals(Object o);
    @Override
    public abstract int hashCode();

    public boolean isPresent() {
        return true;
    }

    /** Internal use: integer used during comparison, unique per implementation type, to allow comparison across distinct types. */
    protected abstract int __type_comparison_index();

    private final static class EmptyMetricValue extends MetricValue {
        private EmptyMetricValue() {}

        @Override
        public Optional<Number> value() { return Optional.empty(); }
        @Override
        public Optional<Boolean> asBool() { return Optional.empty(); }
        @Override
        public Optional<String> asString() { return Optional.empty(); }
        @Override
        public boolean isPresent() { return false; }
        @Override
        public int hashCode() { return 0; }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            return other instanceof EmptyMetricValue;
        }
        @Override
        public String configString() { return null; }
        @Override
        protected int __type_comparison_index() { return 0; }
        @Override
        public int compareTo(MetricValue o) {
            if (__type_comparison_index() < o.__type_comparison_index()) return -1;
            if (__type_comparison_index() > o.__type_comparison_index()) return  1;
            return 0;
        }
    }

    private final static class BooleanMetricValue extends MetricValue {
        private final boolean bool_;

        private BooleanMetricValue(boolean bool) { bool_ = bool; }
        @Override
        public Optional<Number> value() { return Optional.of(bool_ ? 1L : 0L); }
        @Override
        public Boolean getBoolValue() { return bool_; }
        @Override
        public Optional<Boolean> asBool() { return Optional.of(bool_); }
        @Override
        public Optional<String> asString() { return Optional.of(bool_ ? "true" : "false"); }
        @Override
        public int hashCode() { return Boolean.hashCode(bool_); }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (!(other instanceof BooleanMetricValue)) return false;
            return bool_ == ((BooleanMetricValue)other).bool_;
        }
        @Override
        public String configString() { return bool_ ? "true" : "false"; }
        @Override
        protected int __type_comparison_index() { return 4; }
        @Override
        public int compareTo(MetricValue o) {
            if (__type_comparison_index() < o.__type_comparison_index()) return -1;
            if (__type_comparison_index() > o.__type_comparison_index()) return  1;
            return Boolean.compare(bool_, ((BooleanMetricValue)o).bool_);
        }
    }

    private final static class IntMetricValue extends MetricValue {
        private final long value_;

        private IntMetricValue(long value) { value_ = value; }
        @Override
        public Optional<Number> value() { return Optional.of(value_); }
        @Override
        public Optional<Boolean> asBool() { return Optional.of(value_ != 0); }
        @Override
        public Long getIntValue() { return value_; }
        @Override
        public Optional<String> asString() { return Optional.of(String.valueOf(value_)); }
        @Override
        public int hashCode() { return Long.hashCode(value_); }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (!(other instanceof IntMetricValue)) return false;
            return value_ == ((IntMetricValue)other).value_;
        }
        @Override
        public String configString() { return String.valueOf(value_); }
        @Override
        protected int __type_comparison_index() { return 3; }
        @Override
        public int compareTo(MetricValue o) {
            if (__type_comparison_index() < o.__type_comparison_index()) return -1;
            if (__type_comparison_index() > o.__type_comparison_index()) return  1;
            return Long.compare(value_, ((IntMetricValue)o).value_);
        }
    }

    private final static class DblMetricValue extends MetricValue {
        private final double value_;

        private DblMetricValue(double value) { value_ = value; }
        @Override
        public Optional<Number> value() { return Optional.of(value_); }
        @Override
        public Double getFltValue() { return value_; }
        @Override
        public Optional<Boolean> asBool() { return Optional.of(Double.compare(value_, 0d) != 0); }
        @Override
        public Optional<String> asString() { return Optional.of(String.valueOf(value_)); }
        @Override
        public int hashCode() { return Double.hashCode(value_); }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (!(other instanceof DblMetricValue)) return false;
            return value_ == ((DblMetricValue)other).value_;
        }
        @Override
        public String configString() { return String.valueOf(value_); }
        @Override
        protected int __type_comparison_index() { return 2; }
        @Override
        public int compareTo(MetricValue o) {
            if (__type_comparison_index() < o.__type_comparison_index()) return -1;
            if (__type_comparison_index() > o.__type_comparison_index()) return  1;
            return Double.compare(value_, ((DblMetricValue)o).value_);
        }
    }

    private final static class StringMetricValue extends MetricValue {
        private final String value_;

        private StringMetricValue(String value) { value_ = requireNonNull(value); }
        @Override
        public Optional<Number> value() { return Optional.empty(); }
        @Override
        public String getStrValue() { return value_; }
        @Override
        public Optional<Boolean> asBool() { return Optional.empty(); }
        @Override
        public Optional<String> asString() { return Optional.of(value_); }
        @Override
        public Optional<String> stringValue() { return Optional.of(value_); }
        @Override
        public int hashCode() { return value_.hashCode(); }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (!(other instanceof StringMetricValue)) return false;
            return value_.equals(((StringMetricValue)other).value_);
        }
        @Override
        public String configString() { return ConfigSupport.quotedString(value_).toString(); }
        @Override
        protected int __type_comparison_index() { return 1; }
        @Override
        public int compareTo(MetricValue o) {
            if (__type_comparison_index() < o.__type_comparison_index()) return -1;
            if (__type_comparison_index() > o.__type_comparison_index()) return  1;
            return value_.compareTo(((StringMetricValue)o).value_);
        }
    }

    private final static class HistogramMetricValue extends MetricValue {
        private final Histogram value_;

        private HistogramMetricValue(Histogram value) { value_ = requireNonNull(value); }
        @Override
        public Optional<Number> value() { return Optional.empty(); }
        @Override
        public Optional<Boolean> asBool() { return Optional.of(!value_.isEmpty()); }
        @Override
        public Optional<String> asString() { return Optional.empty(); }
        @Override
        public Optional<Histogram> histogram() { return Optional.of(value_); }
        @Override
        public Histogram getHistValue() { return value_; }
        @Override
        public int hashCode() { return value_.hashCode(); }
        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (!(other instanceof HistogramMetricValue)) return false;
            return value_.equals(((HistogramMetricValue)other).value_);
        }
        @Override
        public String configString() { return value_.toString(); }
        @Override
        protected int __type_comparison_index() { return 5; }
        @Override
        public int compareTo(MetricValue o) {
            if (__type_comparison_index() < o.__type_comparison_index()) return -1;
            if (__type_comparison_index() > o.__type_comparison_index()) return  1;
            return value_.compareTo(((HistogramMetricValue)o).value_);
        }
    }
}
