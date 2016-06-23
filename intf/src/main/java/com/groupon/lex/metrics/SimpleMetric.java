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

import java.beans.ConstructorProperties;
import java.util.Objects;
import static java.util.Objects.requireNonNull;

/**
 * Create a metric emitting a constant.
 * @author ariane
 */
public class SimpleMetric implements Metric {
    private final MetricName name_;
    private final MetricValue value_;

    /**
     * Construct a new metric with a constant value.
     * @param name The name of the metric.
     * @param value The value of the metric.
     */
    public SimpleMetric(MetricName name, Boolean value) {
        this(name, MetricValue.fromBoolean(requireNonNull(value)));
    }

    /**
     * Construct a new metric with a constant value.
     * @param name The name of the metric.
     * @param value The value of the metric.
     */
    public SimpleMetric(MetricName name, Number value) {
        this(name, MetricValue.fromNumberValue(requireNonNull(value)));
    }

    /**
     * Construct a new metric with a constant value.
     * @param name The name of the metric.
     * @param value The value of the metric.
     */
    public SimpleMetric(MetricName name, String value) {
        this(name, MetricValue.fromStrValue(requireNonNull(value)));
    }

    /**
     * Construct a new metric with a constant value.
     * @param name The name of the metric.
     * @param value The value of the metric.
     */
    @ConstructorProperties({ "name", "value" })
    public SimpleMetric(MetricName name, MetricValue value) {
        name_ = requireNonNull(name);
        value_ = requireNonNull(value);
    }

    @Override
    public MetricName getName() { return name_; }
    @Override
    public MetricValue getValue() { return value_; }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.name_);
        hash = 89 * hash + Objects.hashCode(this.value_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SimpleMetric other = (SimpleMetric) obj;
        if (!Objects.equals(this.name_, other.name_)) {
            return false;
        }
        if (!Objects.equals(this.value_, other.value_)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "SimpleMetric{" + name_ + "=" + value_ + '}';
    }
}
