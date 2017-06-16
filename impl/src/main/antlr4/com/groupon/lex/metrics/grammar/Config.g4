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
grammar Config;
options {
    tokenVocab=ConfigTokenizer;
}
import ConfigBnf;

@parser::header {
    import com.groupon.lex.metrics.lib.TriFunction;
    import com.groupon.lex.metrics.config.*;
    import com.groupon.lex.metrics.expression.*;
    import com.groupon.lex.metrics.timeseries.*;
    import com.groupon.lex.metrics.timeseries.expression.*;
    import com.groupon.lex.metrics.MetricValue;
    import com.groupon.lex.metrics.GroupName;
    import com.groupon.lex.metrics.PathMatcher;
    import com.groupon.lex.metrics.MetricMatcher;
    import com.groupon.lex.metrics.MetricName;
    import com.groupon.lex.metrics.SimpleGroupPath;
    import com.groupon.lex.metrics.Histogram;
    import com.groupon.lex.metrics.transformers.NameResolver;
    import com.groupon.lex.metrics.transformers.LiteralNameResolver;
    import com.groupon.lex.metrics.transformers.IdentifierNameResolver;
    import com.groupon.lex.metrics.resolver.*;
    import com.groupon.lex.metrics.builders.collector.*;
    import java.util.Objects;
    import java.util.SortedSet;
    import java.util.TreeSet;
    import java.util.ArrayList;
    import java.util.Collection;
    import java.util.Collections;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.Deque;
    import java.util.ArrayDeque;
    import java.io.File;
    import javax.management.ObjectName;
    import javax.management.MalformedObjectNameException;
    import java.util.function.Function;
    import java.util.function.BiFunction;
    import java.util.function.Consumer;
    import java.util.Optional;
    import org.joda.time.Duration;
    import com.groupon.lex.metrics.lib.Any2;
    import com.groupon.lex.metrics.lib.Any3;
}


expr             returns [ Configuration s ]
                 : s1=import_statements s2=collect_statements s3=rules EOF
                   { $s = new Configuration($s1.s, $s2.s, $s3.s); }
                 ;
