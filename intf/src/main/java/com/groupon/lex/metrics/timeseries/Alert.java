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
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.Any2;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class Alert implements Serializable {
    private final GroupName name_;
    private final transient Supplier<CharSequence> rule_;
    private final DateTime start_, cur_;
    private final Optional<Boolean> triggered_;
    private final Duration fire_duration_;
    private final String message_;
    private final Map<String, Any2<MetricValue, List<MetricValue>>> attributes_;

    public Alert(DateTime start, GroupName name, Supplier<CharSequence> rule, Optional<Boolean> triggered, Duration fire_duration, String message, Map<String, Any2<MetricValue, List<MetricValue>>> attributes) {
        this(start, start, name, rule, triggered, fire_duration, message, attributes);
    }

    public Alert(DateTime start, DateTime cur, GroupName name, Supplier<CharSequence> rule, Optional<Boolean> triggered, Duration fire_duration, String message, Map<String, Any2<MetricValue, List<MetricValue>>> attributes) {
        start_ = start;
        cur_ = cur;
        name_ = name;
        rule_ = rule;
        triggered_ = triggered;
        fire_duration_ = fire_duration;
        message_ = message;
        attributes_ = attributes;
    }

    public GroupName getName() { return name_; }
    public String getRule() { return rule_.get().toString(); }
    public DateTime getStart() { return start_; }
    public DateTime getCur() { return cur_; }
    public Duration getDuration() { return new Duration(getStart(), getCur()); }
    public Duration getFireDuration() { return fire_duration_; }
    public Optional<Boolean> isTriggered() { return triggered_; }
    public boolean isFiring() { return isTriggered().orElse(Boolean.FALSE) && !getDuration().isShorterThan(getFireDuration()); }
    public String getMessage() { return message_; }
    public Map<String, Any2<MetricValue, List<MetricValue>>> getAttributes() { return attributes_; }

    public AlertState getAlertState() {
        if (isFiring()) return AlertState.FIRING;
        return isTriggered()
                .map((triggered) -> {
                    return (triggered ? AlertState.TRIGGERING : AlertState.OK);
                })
                .orElse(AlertState.UNKNOWN);
    }

    /**
     * Combine this alert with a new alert.
     * @param new_alert The new alert that matches this alert.
     * @return The new alert, if the trigger state mismatches.
     *     If the trigger state matches, a copy of the new alert is returned, with the start time of the old alert.
     *     If the new triggered state is unknown, the duration will be extended unless the previous state was not-triggered.
     */
    public Alert extend(Alert new_alert) {
        final DateTime new_start;
        if (new_alert.triggered_.orElse(true).equals(triggered_.orElse(true)))
            new_start = start_;
        else
            new_start = new_alert.getStart();
        return new Alert(new_start, new_alert.cur_, new_alert.name_, new_alert.rule_, new_alert.triggered_, new_alert.fire_duration_, new_alert.message_, new_alert.attributes_);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 71 * hash + Objects.hashCode(this.name_);
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
        final Alert other = (Alert) obj;
        if (!Objects.equals(this.name_, other.name_)) {
            return false;
        }
        return true;
    }
}
