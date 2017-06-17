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
parser grammar ConfigBnf;

/*
 * NOTE: include the following in your parser extension:
 *
 * @parser::header {
 *     package com.groupon.lex.metrics.grammar;
 *
 *     import com.groupon.lex.metrics.lib.TriFunction;
 *     import com.groupon.lex.metrics.config.*;
 *     import com.groupon.lex.metrics.expression.*;
 *     import com.groupon.lex.metrics.timeseries.*;
 *     import com.groupon.lex.metrics.timeseries.expression.*;
 *     import com.groupon.lex.metrics.MetricValue;
 *     import com.groupon.lex.metrics.GroupName;
 *     import com.groupon.lex.metrics.PathMatcher;
 *     import com.groupon.lex.metrics.MetricMatcher;
 *     import com.groupon.lex.metrics.MetricName;
 *     import com.groupon.lex.metrics.SimpleGroupPath;
 *     import com.groupon.lex.metrics.Histogram;
 *     import com.groupon.lex.metrics.transformers.NameResolver;
 *     import com.groupon.lex.metrics.transformers.LiteralNameResolver;
 *     import com.groupon.lex.metrics.transformers.IdentifierNameResolver;
 *     import com.groupon.lex.metrics.resolver.*;
 *     import com.groupon.lex.metrics.builders.collector.*;
 *     import java.util.Objects;
 *     import java.util.SortedSet;
 *     import java.util.TreeSet;
 *     import java.util.ArrayList;
 *     import java.util.Collection;
 *     import java.util.Collections;
 *     import java.util.Map;
 *     import java.util.HashMap;
 *     import java.util.Deque;
 *     import java.util.ArrayDeque;
 *     import java.io.File;
 *     import javax.management.ObjectName;
 *     import javax.management.MalformedObjectNameException;
 *     import java.util.function.Function;
 *     import java.util.function.BiFunction;
 *     import java.util.function.Consumer;
 *     import java.util.Optional;
 *     import org.joda.time.Duration;
 *     import com.groupon.lex.metrics.lib.Any2;
 *     import com.groupon.lex.metrics.lib.Any3;
 * }
 */

import ExprBnf;

@members {
    private File dir_;

    public File getDir() { return dir_; }
    public void setDir(File dir) { dir_ = dir; }
}

configuration    returns [ Configuration s ]
                 @after{ $s = new Configuration(Objects.requireNonNull($s1.s), Objects.requireNonNull($s2.s), Objects.requireNonNull($s3.s)); }
                 : s1=import_statements s2=collect_statements s3=rules EOF
                 ;

import_statements returns [ Collection<ImportStatement> s = new ArrayList<ImportStatement>() ]
                 : (s1=import_statement{ $s.add($s1.s); })*
                 ;
import_statement returns [ ImportStatement s ]
                 @after{ $s = new ImportStatement(getDir(), Objects.requireNonNull($s4.s), Objects.requireNonNull($s2.s)); }
                 : IMPORT_KW s2=import_selectors FROM_KW s4=filename ENDSTATEMENT_KW
                 ;
import_selectors returns [ int s ]
                 : ALL_KW
                   { $s = ImportStatement.ALL; }
                 | (s_=import_selector{ $s |= $s_.s; }) (COMMA_LIT s_=import_selector{ $s |= $s_.s; })*
                 ;
import_selector returns [ int s ]
                 : COLLECTORS_KW
                   { $s = ImportStatement.MONITORS; }
                 ;

collect_statements returns [ Collection<MonitorStatement> s = new ArrayList<MonitorStatement>() ]
                 : (s1=collect_statement{ $s.add($s1.s); })*
                 ;
collect_statement returns [ MonitorStatement s ]
                 @init{
                   String builderName = null;
                   CollectorBuilder builderInst = null;
                 }
                 : COLLECT_KW id=ID
                   {
                     builderName = $id.getText();
                     try {
                       builderInst = Configuration.COLLECTORS.get(builderName).newInstance();
                     } catch (InstantiationException | IllegalAccessException ex) {
                       throw new FailedPredicateException(this, "collector " + builderName + " is not instantiable", ex.getMessage());
                     }
                   }
                   collect_stmt_parse[ builderInst ]
                   { $s = new CollectorBuilderWrapper(builderName, builderInst); }
                 ;
collect_stmt_parse [ CollectorBuilder builder ]
                 : collect_stmt_parse_main[ $builder ]
                   collect_stmt_parse_asPath[ $builder ]
                   collect_stmt_parse_tagSet[ $builder ]
                 ;
collect_stmt_parse_main [ CollectorBuilder builder ]
                 @init{
                   List<String> strings;
                 }
                 : ( { $builder instanceof MainNone }?
                     /* SKIP */
                   | { $builder instanceof MainString }?
                     main_name=QSTRING
                     { ((MainString)$builder).setMain($main_name.text); }
                   | { $builder instanceof MainStringList }?
                     main_name0=QSTRING
                     { strings = new ArrayList<>();
                       strings.add($main_name0.text);
                     }
                     ( COMMA_LIT main_nameN=QSTRING
                       { strings.add($main_nameN.text); }
                     )*
                     { ((MainStringList)$builder).setMain(strings); }
                   )
                 ;
collect_stmt_parse_asPath [ CollectorBuilder builder ]
                 : ( { $builder instanceof AcceptAsPath }?
                     AS_KW grp=lit_group_name
                     { ((AcceptAsPath)$builder).setAsPath($grp.s); }
                   | { $builder instanceof AcceptOptAsPath }?
                     ( AS_KW grp=lit_group_name
                       { ((AcceptOptAsPath)$builder).setAsPath(Optional.of($grp.s)); }
                     | { ((AcceptOptAsPath)$builder).setAsPath(Optional.empty()); }
                     )
                   | /* SKIP */
                   )
                 ;
collect_stmt_parse_tagSet [ CollectorBuilder builder ]
                 : ( { $builder instanceof AcceptTagSet }?
                     tuples=opt_tuple_body
                     { ((AcceptTagSet)$builder).setTagSet($tuples.s); }
                   | ENDSTATEMENT_KW
                     /* SKIP */
                   )
                 ;

rules            returns [ Collection<RuleStatement> s = new ArrayList<>() ]
                 : (s1=monsoon_rule{ $s.add($s1.s); })*
                 ;
monsoon_rule     returns [ RuleStatement s ]
                 @init{ RuleStatement result = null; }
                 @after{ $s = Objects.requireNonNull(result); }
                 : s_rule=match_rule
                   { result = $s_rule.s; }
                 | s_const=constant_statement
                   { result = $s_const.s; }
                 | s_alert=alert_statement
                   { result = $s_alert.s; }
                 | s_alias=alias_rule
                   { result = $s_alias.s; }
                 | s_derived=derived_metric_rule
                   { result = $s_derived.s; }
                 | s_tag=tag_rule
                   { result = $s_tag.s; }
                 ;

constant_statement returns [ RuleStatement s ]
                 @init{ Map<NameResolver, MetricValue> metrics = new HashMap<>(); }
                 @after{ $s = new ResolvedConstantStatement($s2.s, metrics); }
                 : CONSTANT_KW s2=group s3=metric_name s4=metric_constant ENDSTATEMENT_KW
                   { metrics.put($s3.s, $s4.s); }
                 | CONSTANT_KW s2=group
                   CURLYBRACKET_OPEN
                   (s4_map=constant_stmt_metrics{ metrics.putAll($s4_map.s); })+
                   CURLYBRACKET_CLOSE
                 ;
constant_stmt_metrics returns [ Map<NameResolver, MetricValue> s ]
                 @after{ $s = Collections.singletonMap($s2.s, $s3.s); }
                 : s2=metric_name s3=metric_constant ENDSTATEMENT_KW
                 ;

match_rule       returns [ RuleStatement s ]
                 @init{
                   Map<String, PathMatcher> g_matchers = new HashMap<>();
                   Map<MatchStatement.IdentifierPair, MetricMatcher> m_matchers = new HashMap<>();
                   Optional<TimeSeriesMetricExpression> where_clause = Optional.empty();
                   MutableScope current_scope = newMutableScope();
                 }
                 : MATCH_KW
                   match_rule_selector[g_matchers, m_matchers]
                   ( COMMA_LIT match_rule_selector[g_matchers, m_matchers])*
                   {
                     g_matchers.keySet().forEach(ident -> current_scope.put(ident, Scope.Type.GROUP));
                     m_matchers.keySet().forEach(ident_pair -> {
                         current_scope.put(ident_pair.getGroup(), Scope.Type.GROUP);
                         current_scope.put(ident_pair.getMetric(), Scope.Type.METRIC);
                     });
                   }
                   ( WHERE_KW s_where=expression{ where_clause = Optional.of($s_where.s); }
                   )?
                   CURLYBRACKET_OPEN
                   s_rules=rules
                   CURLYBRACKET_CLOSE
                   { $s = new MatchStatement(g_matchers, m_matchers, where_clause, $s_rules.s); }
                 ;
                 finally{
                   popScope(current_scope);
                 }
match_rule_selector [ Map<String, PathMatcher> g_matchers,
                      Map<MatchStatement.IdentifierPair, MetricMatcher> m_matchers ]
                 : ( s2_group=path_match AS_KW s4=identifier
                     { $g_matchers.put($s4.s, $s2_group.s); }
                   | s2_metric=metric_match AS_KW s4=identifier COMMA_LIT s6=identifier
                     { $m_matchers.put(new MatchStatement.IdentifierPair($s4.s, $s6.s), $s2_metric.s); }
                   )
                 ;

alias_rule       returns [ AliasStatement s ]
                 : ALIAS_KW s2=name AS_KW s4=identifier ENDSTATEMENT_KW
                   {
                     $s = new AliasStatement($s4.s, $s2.s);
                     currentMutableScope().put($s4.s, Scope.Type.GROUP);
                   }
                 ;

derived_metric_rule returns [ DerivedMetricStatement s ]
                 @init{
                     Map<NameResolver, TimeSeriesMetricExpression> metrics = new HashMap<>();
                     Map<String, TimeSeriesMetricExpression> tags = new HashMap<>();
                 }
                 : DEFINE_KW s2=name
                   (
                     TAG_KW BRACE_OPEN_LIT
                     tag_name0=identifier EQ_KW tag_value0=expression
                     { tags.put($tag_name0.s, $tag_value0.s); }
                     ( COMMA_LIT tag_nameN=identifier EQ_KW tag_valueN=expression
                       { tags.put($tag_nameN.s, $tag_valueN.s); }
                     )*
                     BRACE_CLOSE_LIT
                   )?

                   ( s3=metric_name EQ_KW s5=expression ENDSTATEMENT_KW
                     { metrics = Collections.singletonMap($s3.s, $s5.s); }
                   | CURLYBRACKET_OPEN
                     (m=derived_metric_rule_metrics{ metrics.putAll($m.s); })*
                     CURLYBRACKET_CLOSE
                   )
                   { $s = new DerivedMetricStatement($s2.s, tags, metrics); }
                 ;
derived_metric_rule_metrics returns [ Map<NameResolver, TimeSeriesMetricExpression> s ]
                 @after{ $s = Collections.singletonMap($s1.s, $s3.s); }
                 : s1=metric_name EQ_KW s3=expression ENDSTATEMENT_KW
                 ;

tag_rule         returns [ SetTagStatement s ]
                 @init{ Map<String, TimeSeriesMetricExpression> tag_exprs = new HashMap<>(); }
                 : TAG_KW s2=group
                   ( AS_KW s4=identifier EQ_KW s6=expression ENDSTATEMENT_KW
                     {
                       $s = new SetTagStatement($s2.s, $s4.s, $s6.s);
                     }
                   | CURLYBRACKET_OPEN
                     s4_0=identifier EQ_KW s6_0=expression{ tag_exprs.put($s4_0.s, $s6_0.s); }
                     (COMMA_LIT s4_n=identifier EQ_KW s6_n=expression{ tag_exprs.put($s4_n.s, $s6_n.s); })*
                     CURLYBRACKET_CLOSE
                     { $s = new SetTagStatement($s2.s, tag_exprs); }
                   )
                 ;

filename         returns [ String s ]
                 : s1=QSTRING
                   { $s = $s1.text; }
                 ;
dotted_identifier returns [ String s ]
                 : s1=raw_dotted_identifier
                   { $s = String.join(".", $s1.s); }
                 ;
raw_dotted_identifier returns [ List<String> s = new ArrayList<String>() ]
                 : s1=identifier{ $s.add($s1.s); } (DOT_LIT s_=identifier{ $s.add($s_.s); })*
                 ;
lit_group_name   returns [ SimpleGroupPath s ]
                 : s1=raw_dotted_identifier
                   { $s = SimpleGroupPath.valueOf($s1.s); }
                 ;
metric_name      returns [ NameResolver s ]
                 : s1=name
                   { $s = $s1.s; }
                 ;
metric_constant  returns [ MetricValue s ]
                 : s1_str=QSTRING
                   { $s = MetricValue.fromStrValue($s1_str.text); }
                 | s1_int=int_val
                   { $s = MetricValue.fromIntValue($s1_int.s); }
                 | s1_dbl=fp_val
                   { $s = MetricValue.fromDblValue($s1_dbl.s); }
                 | TRUE_KW
                   { $s = MetricValue.TRUE; }
                 | FALSE_KW
                   { $s = MetricValue.FALSE; }
                 ;

alert_statement  returns [ AlertStatement s ]
                 @init{ Map<String, Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>>> attrs = null; }
                 @after{ $s = new AlertStatement($s2.s, $s4.s, $s5.s, $s6.s, attrs); }
                 : ALERT_KW s2=name IF_KW s4=expression s5=alert_statement_opt_duration s6=alert_statement_opt_message
                   ( ENDSTATEMENT_KW
                     { attrs = Collections.EMPTY_MAP; }
                   | s7=alert_statement_attributes
                     { attrs = $s7.s; }
                   )
                 ;
alert_statement_opt_duration returns [ Optional<Duration> s ]
                 : FOR_KW s2=duration
                   { $s = Optional.of($s2.s); }
                 | /* SKIP */
                   { $s = Optional.empty(); }
                 ;
alert_statement_opt_message returns [ Optional<String> s ]
                 @init{ String s = null; }
                 @after{ $s = Optional.ofNullable(s); }
                 : ( MESSAGE_KW s2=QSTRING
                     { s = $s2.text; }
                   )?
                 ;
alert_statement_attributes returns [ Map<String, Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>>> s = new HashMap<>() ]
                 : ATTRIBUTES_KW CURLYBRACKET_OPEN
                   ( sline_0=alert_statement_attributes_line{ $s.put($sline_0.s.getKey(), $sline_0.s.getValue()); }
                     (COMMA_LIT sline=alert_statement_attributes_line{ $s.put($sline.s.getKey(), $sline.s.getValue()); })*
                   )?
                   CURLYBRACKET_CLOSE
                 ;
alert_statement_attributes_line returns [ Map.Entry<String, Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>>> s ]
                 : s1=identifier EQ_KW s3=alert_statement_attributes_line_arg
                   { $s = com.groupon.lex.metrics.lib.SimpleMapEntry.create($s1.s, $s3.s); }
                 ;
alert_statement_attributes_line_arg returns [ Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>> s ]
                 @init{ List<TimeSeriesMetricExpression> exprs = new ArrayList<>(); }
                 : s_expr=expression
                   { $s = Any2.left($s_expr.s); }
                 | SQBRACE_OPEN_LIT
                   ( s_expr=expression{ exprs.add($s_expr.s); }
                     (COMMA_LIT s_expr=expression{ exprs.add($s_expr.s); })*
                   )
                   SQBRACE_CLOSE_LIT
                   { $s = Any2.right(exprs); }
                 ;

opt_tuple_body   returns [ NameBoundResolver s ]
                 @init{
                   final List<NameBoundResolver> resolvers = new ArrayList<>();
                 }
                 : ENDSTATEMENT_KW
                   { $s = NameBoundResolver.EMPTY; }
                 | CURLYBRACKET_OPEN
                   m0=tuple_line{ resolvers.add($m0.s); }
                   (
                     COMMA_LIT mN=tuple_line{ resolvers.add($mN.s); }
                   )*
                   CURLYBRACKET_CLOSE
                   {
                     if (resolvers.size() == 1)
                         $s = resolvers.get(0);
                     else
                         $s = new NameBoundResolverSet(resolvers);
                   }
                 ;
tuple_line returns [ NameBoundResolver s ]
                 @init{
                   List<Any2<Integer, String>> keys = new ArrayList<>();
                   List<ResolverTuple> tuples = new ArrayList<>();
                 }
                 : ( s_tk=tuple_key { keys = Collections.singletonList($s_tk.s); }
                     EQ_KW
                     SQBRACE_OPEN_LIT
                     s0=tuple_value{ tuples.add(new ResolverTuple($s0.s)); }
                     (COMMA_LIT sN=tuple_value{ tuples.add(new ResolverTuple($sN.s)); })*
                     SQBRACE_CLOSE_LIT
                   | keys=tuple_line_key_tuple
                     { keys = $keys.s; }
                     EQ_KW
                     SQBRACE_OPEN_LIT
                     values=tuple_line_value_tuple
                     { tuples.add(new ResolverTuple($values.s)); }
                     ( COMMA_LIT values=tuple_line_value_tuple
                       { tuples.add(new ResolverTuple($values.s)); }
                     )*
                     SQBRACE_CLOSE_LIT
                   )
                   {
                     $s = new SimpleBoundNameResolver(
                            new SimpleBoundNameResolver.Names(keys),
                            new ConstResolver(tuples));
                   }
                 ;
tuple_line_key_tuple returns [ List<Any2<Integer, String>> s = new ArrayList<>() ]
                 : BRACE_OPEN_LIT
                   s1=tuple_key { $s.add($s1.s); }
                   ( COMMA_LIT
                     sN=tuple_key { $s.add($sN.s); }
                   )*
                   BRACE_CLOSE_LIT
                 ;
tuple_line_value_tuple returns [ List<Any3<Boolean, Integer, String>> s = new ArrayList<>() ]
                 : BRACE_OPEN_LIT
                   s0=tuple_value{ $s.add($s0.s); }
                   ( COMMA_LIT
                     sN=tuple_value{ $s.add($sN.s); }
                   )*
                   BRACE_CLOSE_LIT
                 ;
tuple_key        returns [ Any2<Integer, String> s ]
                 : s_i=uint_val      { $s = Any2.left((int)$s_i.s); }
                 | s_s=identifier    { $s = Any2.right($s_s.s); }
                 ;
tuple_value      returns [ Any3<Boolean, Integer, String> s ]
                 : TRUE_KW           { $s = ResolverTuple.newTupleElement(true);   }
                 | FALSE_KW          { $s = ResolverTuple.newTupleElement(false);  }
                 | s_i=int_val       { $s = ResolverTuple.newTupleElement((int)$s_i.s); }
                 | s_s=QSTRING       { $s = ResolverTuple.newTupleElement($s_s.text); }
                 ;
