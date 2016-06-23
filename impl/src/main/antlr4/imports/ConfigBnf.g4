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
 *     import com.groupon.lex.metrics.NameCache;
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
 * }
 */

@members {
    private File dir_;

    public File getDir() { return dir_; }
    public void setDir(File dir) { dir_ = dir; }

    private final Deque<Scope> scopes_ = new ArrayDeque<>(Collections.singleton(new MutableScope()));
    private Scope currentScope() { return scopes_.peek(); }
    private MutableScope currentMutableScope() {
        Scope scope = currentScope();
        return (MutableScope)scope;
    }
    private void pushScope(Scope scope) { scopes_.addFirst(scope); }
    private void popScope(Scope scope) {
        if (scopes_.peek() != scope) throw new IllegalStateException("Scope mismatch");
        scopes_.removeFirst();
    }
    private MutableScope newMutableScope() {
        MutableScope new_scope = new MutableScope(currentScope());
        pushScope(new_scope);
        return new_scope;
    }

    private Consumer<String> error_message_consumer_ = (String x) -> {};
    public void setErrorMessageConsumer(Consumer<String> error_message_consumer) {
        error_message_consumer_ = error_message_consumer;
    }
    public void emitErrorMessage(String msg) {
        error_message_consumer_.accept(msg);
    }
}

expr             returns [ Configuration s ]
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
                 : COLLECT_KW id=ID
                   ( { $id.getText().equals("jmx_listener") }?  s_jmx=collect_jmx_listener
                     { $s = $s_jmx.s; }
                   | { $id.getText().equals("url") }?           s_url=collect_url
                     { $s = $s_url.s; }
                   | { $id.getText().equals("json_url") }?      s_json_url=collect_json_url
                     { $s = $s_json_url.s; }
                   | { $id.getText().equals("collectd_push") }? s_name=quoted_string AS_KW s_grp=lit_group_name ENDSTATEMENT_KW
                     { $s = new CollectdPushMonitor($s_name.s, $s_grp.s); }
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
                 @init{ Map<NameResolver, TimeSeriesMetricExpression> metrics = new HashMap<>(); }
                 : DEFINE_KW s2=name
                   ( s3=metric_name EQ_KW s5=expression ENDSTATEMENT_KW
                     { $s = new DerivedMetricStatement($s2.s, $s3.s, $s5.s); }
                   | CURLYBRACKET_OPEN
                     (m=derived_metric_rule_metrics{ metrics.putAll($m.s); })*
                     CURLYBRACKET_CLOSE
                     { $s = new DerivedMetricStatement($s2.s, metrics); }
                   )
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
                 : s1=quoted_string
                   { $s = $s1.s; }
                 ;
identifier       returns [ String s ]
                 : s1_tok=ID
                   { $s = $s1_tok.getText(); }
                 | s1_str=quoted_identifier
                   { $s = $s1_str.s; }
                 ;
dotted_identifier returns [ String s ]
                 : s1=raw_dotted_identifier
                   { $s = String.join(".", $s1.s); }
                 ;
raw_dotted_identifier returns [ List<String> s = new ArrayList<String>() ]
                 : s1=identifier{ $s.add($s1.s); } (DOT_LIT s_=identifier{ $s.add($s_.s); })*
                 ;
group            returns [ GroupExpression s ]
                 @init{ NameResolver tmp; }
                 : s_var=ID
                   ( { currentScope().isIdentifier($s_var.getText(), Scope.Type.GROUP) }?
                     { $s = new IdentifierGroupExpression($s_var.getText()); }
                   | { !currentScope().isIdentifier($s_var.getText(), Scope.Type.GROUP) }?
                     { tmp = new LiteralNameResolver($s_var.getText()); }
                     ( DOT_LIT gn=name
                       { tmp = NameResolver.combine(tmp, $gn.s); }
                     )?
                     { $s = new LiteralGroupExpression(tmp); }
                   )
                 | // Match if none of the above matches.
                   s_rdi=name
                   { $s = new LiteralGroupExpression($s_rdi.s); }
                 ;
lit_group_name   returns [ SimpleGroupPath s ]
                 : s1=raw_dotted_identifier
                   { $s = NameCache.singleton.newSimpleGroupPath($s1.s); }
                 ;
name             returns [ NameResolver s ]
                 @init{
                   NameResolver resolver;
                 }
                 : s0=name_elem{ resolver = $s0.s; }
                   ( DOT_LIT s_=name_elem
                     { resolver = NameResolver.combine(resolver, $s_.s); }
                   )*
                   { $s = resolver; }
                 ;
name_elem        returns [ NameResolver s ]
                 : DOLLAR_LIT CURLYBRACKET_OPEN s3=identifier s4=name_subselect CURLYBRACKET_CLOSE
                   { $s = new IdentifierNameResolver($s3.s, $s4.s); }
                 | s1=identifier
                   { $s = new LiteralNameResolver($s1.s); }
                 ;
name_subselect returns [ Optional<IdentifierNameResolver.SubSelect> s = Optional.empty() ]
                 @init{
                   Optional<Integer> b = Optional.empty();
                   Optional<Integer> e = Optional.empty();
                 }
                 : ( SQBRACE_OPEN_LIT
                     (s_b=int_val { b = Optional.of((int)$s_b.s); })?
                     ( COLON_LIT (s_e=int_val { e = Optional.of((int)$s_e.s); })?
                       {
                         if (b.isPresent() || e.isPresent())
                             $s = Optional.of(new IdentifierNameResolver.SubSelectRange(b, e));
                       }
                     | { $s = b.map(IdentifierNameResolver.SubSelectIndex::new); }
                     )
                     SQBRACE_CLOSE_LIT
                   )?
                 ;
metric_name      returns [ NameResolver s ]
                 : s1=name
                   { $s = $s1.s; }
                 ;
path_match       returns [ PathMatcher s ]
                 @init{ List<PathMatcher.IdentifierMatch> fragments = new ArrayList<>(); }
                 : ( s_lit=identifier{ fragments.add(new PathMatcher.LiteralNameMatch($s_lit.s)); }
                   | STAR{ fragments.add(new PathMatcher.WildcardMatch()); }
                   | STAR_STAR{ fragments.add(new PathMatcher.DoubleWildcardMatch()); }
                   | s_regex=regex{ fragments.add(new PathMatcher.RegexMatch($s_regex.s)); }
                   )
                   ( DOT_LIT
                     ( s_lit=identifier{ fragments.add(new PathMatcher.LiteralNameMatch($s_lit.s)); }
                     | STAR{ fragments.add(new PathMatcher.WildcardMatch()); }
                     | STAR_STAR{ fragments.add(new PathMatcher.DoubleWildcardMatch()); }
                     | s_regex=regex{ fragments.add(new PathMatcher.RegexMatch($s_regex.s)); }
                     )
                   )*
                   { $s = new PathMatcher(fragments); }
                 ;
metric_match     returns [ MetricMatcher s ]
                 : s1=path_match s2=path_match
                   { $s = new MetricMatcher($s1.s, $s2.s); }
                 ;
metric_constant  returns [ MetricValue s ]
                 : s1_str=quoted_string
                   { $s = MetricValue.fromStrValue($s1_str.s); }
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
                 : (MESSAGE_KW s2=quoted_string)?
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

collect_jmx_listener returns [ MonitorStatement s ]
                 : s1=object_names s2=opt_tuple_body
                   { $s = new JmxListenerMonitor($s1.s, $s2.s); }
                 ;
object_names     returns [ SortedSet<ObjectName> s = new TreeSet<ObjectName>() ]
                 : (s1=object_name{ $s.add($s1.s); } (COMMA_LIT s3=object_name{ $s.add($s3.s); })*)
                 ;
object_name      returns [ ObjectName s ]
                 : s1=quoted_string
                   {
                     try {
                         $s = new ObjectName($s1.s);
                     } catch (MalformedObjectNameException ex) {
                         throw new FailedPredicateException(this, $s1.s + " is not a valid object name", ex.getMessage());
                     }
                   }
                 ;

collect_url      returns [ MonitorStatement s ]
                 : s1=quoted_string AS_KW s3=lit_group_name s4=opt_tuple_body
                   { $s = new UrlGetCollectorMonitor($s3.s, $s1.s, $s4.s); }
                 ;
collect_json_url returns [ MonitorStatement s ]
                 : s1=quoted_string AS_KW s3=lit_group_name s4=opt_tuple_body
                   { $s = new JsonUrlMonitor($s3.s, $s1.s, $s4.s); }
                 ;
opt_tuple_body   returns [ Collection<com.groupon.lex.metrics.TupledElements> s = new ArrayList<>() ]
                 : ENDSTATEMENT_KW
                   { /* SKIP */ }
                 | CURLYBRACKET_OPEN
                   m0=collect_url_line{ $s.add($m0.s); }
                   (
                     COMMA_LIT mN=collect_url_line{ $s.add($mN.s); }
                   )*
                   CURLYBRACKET_CLOSE
                 ;
collect_url_line returns [ com.groupon.lex.metrics.TupledElements s ]
                 : ( s_idx=uint_val
                     {
                       $s = new com.groupon.lex.metrics.TupledElements(Collections.singletonList(Any2.right((int)$s_idx.s)));
                     }
                   | s_tag=identifier
                     {
                       $s = new com.groupon.lex.metrics.TupledElements(Collections.singletonList(Any2.left($s_tag.s)));
                     }
                   )
                   EQ_KW
                   SQBRACE_OPEN_LIT
                   s0=quoted_string{ $s.addValues(Collections.singletonList($s0.s)); }
                   (COMMA_LIT sN=quoted_string{ $s.addValues(Collections.singletonList($sN.s)); })*
                   SQBRACE_CLOSE_LIT
                 | keys=collect_url_line_key_tuple
                   { $s = new com.groupon.lex.metrics.TupledElements($keys.s); }
                   EQ_KW
                   SQBRACE_OPEN_LIT
                   values=collect_url_line_value_tuple
                   { $values.s.size() == $keys.s.size() }?
                   { $s.addValues($values.s); }
                   ( COMMA_LIT values=collect_url_line_value_tuple
                     { $values.s.size() == $keys.s.size() }?
                     { $s.addValues($values.s); }
                   )*
                   SQBRACE_CLOSE_LIT
                 ;
collect_url_line_key_tuple returns [ List<Any2<String, Integer>> s = new ArrayList<>() ]
                 : BRACE_OPEN_LIT
                   ( s_idx=uint_val { $s.add(Any2.right((int)$s_idx.s)); }
                   | s_tag=identifier { $s.add(Any2.left($s_tag.s)); }
                   )
                   ( COMMA_LIT
                     ( s_idx=uint_val { $s.add(Any2.right((int)$s_idx.s)); }
                     | s_tag=identifier { $s.add(Any2.left($s_tag.s)); }
                     )
                   )*
                   BRACE_CLOSE_LIT
                 ;
collect_url_line_value_tuple returns [ List<String> s = new ArrayList<>() ]
                 : BRACE_OPEN_LIT
                   s0=quoted_string{ $s.add($s0.s); }
                   ( COMMA_LIT
                     sN=quoted_string{ $s.add($sN.s); }
                   )*
                   BRACE_CLOSE_LIT
                 ;

/*
 * Number logic.
 *
 * uint_val: accepts any unsigned integer -> returning long
 * int_val: accepts any integer -> returning long
 * fp_val: accepts any floating point value, _except_ integers -> returning double
 * number: accepts any floating point value, including integers -> returning java.lang.Number
 */

int_val          returns [ long s ]
                 : DASH_LIT s2=uint_val
                   { $s = -$s2.s; }
                 | s1=uint_val
                   { $s = $s1.s; }
                 ;
uint_val         returns [ long s ]
                 : s1=DIGITS
                   { $s = Long.parseLong($s1.getText()); }
                 | s1=HEXDIGITS
                   { $s = Long.parseLong($s1.getText()); }
                 | s1=OCTDIGITS
                   { $s = Long.parseLong($s1.getText()); }
                 ;
positive_fp_val  returns [ double s ]
                 : s1=FP_DECIMAL
                   { $s = Double.parseDouble($s1.getText()); }
                 | s1=FP_HEX
                   { $s = Double.parseDouble($s1.getText()); }
                 ;
fp_val           returns [ double s ]
                 : s1=positive_fp_val
                   { $s = $s1.s; }
                 | DASH_LIT s2=positive_fp_val
                   { $s = -$s2.s; }
                 ;
positive_number  returns [ Number s ]
                 : s1_dbl=positive_fp_val
                   { $s = $s1_dbl.s; }
                 | s1_int=uint_val
                   { $s = $s1_int.s; }
                 ;
number           returns [ Number s ]
                 : s1_dbl=fp_val
                   { $s = $s1_dbl.s; }
                 | s1_int=int_val
                   { $s = $s1_int.s; }
                 ;
histogram        returns [ Histogram s ]
                 @init{ List<Histogram.RangeWithCount> elems = new ArrayList<>(); }
                 : SQBRACE_OPEN_LIT
                   ( s0=histogram_elem{ elems.add($s0.s); }
                     (COMMA_LIT sN=histogram_elem{ elems.add($sN.s); })*
                   )?
                   SQBRACE_CLOSE_LIT
                   { $s = new Histogram(elems.stream()); }
                 ;
histogram_elem   returns [ Histogram.RangeWithCount s ]
                 : s_floor=number DOT_DOT_LIT s_ceil=number EQ_KW s_count=number
                   {
                     $s = new Histogram.RangeWithCount(new Histogram.Range($s_floor.s.doubleValue(), $s_ceil.s.doubleValue()), $s_count.s.doubleValue());
                   }
                 ;

/*
 * String logic.
 *
 * Strings are enclosed in double quotes and may contain escape sequences.
 * Strings are sensitive to white space.
 */

quoted_string    returns [ String s = "" ]
                 : BEGIN_QUOTE s2=qstring_raw END_QUOTE
                   { $s = $s2.s.toString(); }
                 ;
quoted_identifier returns [ String s = "" ]
                 : BEGIN_SQUOTE s2=qstring_raw END_QUOTE
                   { $s = $s2.s.toString(); }
                 ;
regex            returns [ String s = "" ]
                 : BEGIN_REGEX s2=qstring_raw END_QUOTE
                   { $s = $s2.s.toString(); }
                 ;
qstring_raw      returns [ StringBuilder s = new StringBuilder() ]
                 : ( s1=RAW{ $s.append($s1.getText()); }
                   | s2=ESC_CHAR{ $s.append($s2.getText()); }
                   )*
                 ;


/*
 * Arithmatic expressions.
 */

primary_expression returns [ TimeSeriesMetricExpression s ]
                 @init{ TimeSeriesMetricExpression expr = null; }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=metric_selector
                   { expr = $s1.s; }
                 | s1_number=constant
                   { expr = Util.constantExpression($s1_number.s); }
                 | TRUE_KW
                   { expr = TimeSeriesMetricExpression.TRUE; }
                 | FALSE_KW
                   { expr = TimeSeriesMetricExpression.FALSE; }
                 | BRACE_OPEN_LIT s2=expression BRACE_CLOSE_LIT
                   { expr = $s2.s; }
                 | s1_string=quoted_string
                   { expr = Util.constantExpression($s1_string.s); }
                 | s1_fn=function_invocation
                   { expr = $s1_fn.s; }
                 | s1_hist=histogram
                   { expr = Util.constantExpression($s1_hist.s); }
                 ;
unary_expression returns [ TimeSeriesMetricExpression s ]
                 @init{ TimeSeriesMetricExpression expr = null; }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=primary_expression
                   { expr = $s1.s; }
                 | factory=unary_operator s2=unary_expression
                   { expr = $factory.s.apply($s2.s); }
                 ;
multiplicative_expression returns [ TimeSeriesMetricExpression s ]
                 @init{
                   TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> transform = null;
                   TimeSeriesMetricExpression expr;
                 }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=unary_expression
                   { expr = $s1.s; }
                   ( ( STAR{ transform = UtilX.multiply(); }
                     | SLASH_LIT{ transform = UtilX.divide(); }
                     | PERCENT_LIT{ transform = UtilX.modulo(); }
                     )
                     matcher=by_match_clause
                     s3=unary_expression { expr = transform.apply(expr, $s3.s, $matcher.s); }
                   )*
                 ;
additive_expression returns [ TimeSeriesMetricExpression s ]
                 @init{
                   TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> transform = null;
                   TimeSeriesMetricExpression expr;
                 }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=multiplicative_expression
                   { expr = $s1.s; }
                   ( ( PLUS_LIT{ transform = UtilX.addition(); }
                     | DASH_LIT{ transform = UtilX.subtraction(); }
                     )
                     matcher=by_match_clause
                     s3=multiplicative_expression { expr = transform.apply(expr, $s3.s, $matcher.s); }
                   )*
                 ;
shift_expression returns [ TimeSeriesMetricExpression s ]
                 @init{
                   TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> transform = null;
                   TimeSeriesMetricExpression expr;
                 }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=additive_expression
                   { expr = $s1.s; }
                   ( ( LEFTSHIFT_KW{ transform = UtilX.leftShift(); }
                     | RIGHTSHIFT_KW{ transform = UtilX.rightShift(); }
                     )
                     matcher=by_match_clause
                     s3=additive_expression { expr = transform.apply(expr, $s3.s, $matcher.s); }
                   )*
                 ;
arithmatic_expression returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = Objects.requireNonNull($s1.s); }
                 : s1=shift_expression
                 ;

constant         returns [ Number s ]
                 @after{ $s = Objects.requireNonNull($s1.s); }
                 : s1=positive_number
                 ;
unary_operator   returns [ Function<TimeSeriesMetricExpression, TimeSeriesMetricExpression> s ]
                 : DASH_LIT
                   { $s = UtilX.negateNumberExpression(); }
                 | PLUS_LIT
                   { $s = UtilX.identityExpression(); }
                 | BANG_LIT
                   { $s = UtilX.negateBooleanPredicate(); }
                 ;


/*
 * Logical expressions.
 */

relational_expression returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = Objects.requireNonNull($s1.s); }
                 : s1=arithmatic_expression
                 ;
equality_expression returns [ TimeSeriesMetricExpression s ]
                 @init{
                   TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> transform = null;
                   BiFunction<TimeSeriesMetricExpression, String, TimeSeriesMetricExpression> regex_transform = null;
                   TimeSeriesMetricExpression expr;
                 }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=arithmatic_expression
                   { expr = $s1.s; }
                   (
                     ( ( EQ_KW{ transform = UtilX.equalPredicate(); }
                       | NEQ_KW{ transform = UtilX.notEqualPredicate(); }
                       | LE_KW{ transform = UtilX.numberLessEqualPredicate(); }
                       | GE_KW{ transform = UtilX.numberLargerEqualPredicate(); }
                       | LT_KW{ transform = UtilX.numberLessThanPredicate(); }
                       | GT_KW{ transform = UtilX.numberLargerThanPredicate(); }
                       )
                       matcher=by_match_clause
                       s3=arithmatic_expression{ expr = transform.apply(expr, $s3.s, $matcher.s); }
                     )
                   | ( ( REGEX_MATCH_KW{ regex_transform = UtilX.regexMatch(); }
                       | REGEX_NEGATE_KW{ regex_transform = UtilX.regexMismatch(); }
                       )
                       ( qs=quoted_string
                         { expr = regex_transform.apply(expr, $qs.s); }
                       | re=regex
                         { expr = regex_transform.apply(expr, $re.s); }
                       )
                     )
                   )?
                 ;
logical_expression returns [ TimeSeriesMetricExpression s ]
                 @init{
                   TriFunction<TimeSeriesMetricExpression, TimeSeriesMetricExpression, TagMatchingClause, TimeSeriesMetricExpression> transform = null;
                   TimeSeriesMetricExpression expr;
                 }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : s1=equality_expression
                   { expr = $s1.s; }
                   ( ( LOGICAL_AND_KW{ transform = UtilX.logicalAnd(); }
                     | LOGICAL_OR_KW{ transform = UtilX.logicalOr(); }
                     )
                     matcher=by_match_clause
                     s3=equality_expression
                     { expr = transform.apply(expr, $s3.s, $matcher.s); }
                   )*
                 ;
expression       returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = Objects.requireNonNull($s1.s); }
                 : s1=logical_expression
                 ;


metric_selector  returns [ TimeSeriesMetricExpression s ]
                 @init{ NameResolver tmp; }
                 : s_var=ID
                   ( { currentScope().isIdentifier($s_var.getText(), Scope.Type.METRIC) }?
                     { $s = new IdentifierMetricSelector($s_var.getText()); }
                   | { currentScope().isIdentifier($s_var.getText(), Scope.Type.GROUP) }?
                     s2=name
                     { $s = Util.selector(new IdentifierGroupExpression($s_var.getText()), Objects.requireNonNull($s2.s)); }
                   | { !currentScope().isIdentifier($s_var.getText(), Scope.Type.METRIC) &&
                       !currentScope().isIdentifier($s_var.getText(), Scope.Type.GROUP) }?
                     { tmp = new LiteralNameResolver($s_var.getText()); }
                     ( DOT_LIT gn=name
                       { tmp = NameResolver.combine(tmp, $gn.s); }
                     )?
                     s2=name
                     { $s = Util.selector(new LiteralGroupExpression(tmp), Objects.requireNonNull($s2.s)); }
                   )
                 | // Match non-ID
                   s1=group s2=name
                   { $s = Util.selector(Objects.requireNonNull($s1.s), Objects.requireNonNull($s2.s)); }
                 ;
label_selector
                 @after{ throw new UnsupportedOperationException(); }
                 : identifier
                   ( (EQ_KW|NEQ_KW) constant
                   | (REGEX_MATCH_KW|REGEX_NEGATE_KW) (constant | regex)
                   )?
                 ;

duration_val     returns [ Duration s ]
                 @after{ $s = $fn.fn.apply(Objects.requireNonNull($s1.s)); }
                 : s1=uint_val fn=duration_unit { $fn.fn != null }?
                 ;
duration         returns [ Duration s = Duration.ZERO ]
                 : (s1=duration_val{ $s = $s.withDurationAdded($s1.s, 1); })+
                 ;
duration_unit    returns [ Function<Long, Duration> fn ]
                 : id=ID
                   {
                     if ($id.getText().equals("s"))
                       $fn = Duration::standardSeconds;
                     else if ($id.getText().equals("m"))
                       $fn = Duration::standardMinutes;
                     else if ($id.getText().equals("h"))
                       $fn = Duration::standardHours;
                     else if ($id.getText().equals("d"))
                       $fn = Duration::standardDays;
                     else
                       $fn = null;
                   }
                 ;


function_invocation returns [ TimeSeriesMetricExpression s ]
                 @init{ TimeSeriesMetricExpression expr = null; }
                 @after{ $s = Objects.requireNonNull(expr); }
                 : fn=ID BRACE_OPEN_LIT
                   ( { $fn.getText().equals("rate")           }? s_fn__rate=fn__rate       { expr = $s_fn__rate.s;    }
                   | { $fn.getText().equals("sum")            }? s_fn__sum=fn__sum         { expr = $s_fn__sum.s;     }
                   | { $fn.getText().equals("avg")            }? s_fn__avg=fn__avg         { expr = $s_fn__avg.s;     }
                   | { $fn.getText().equals("min")            }? s_fn__min=fn__min         { expr = $s_fn__min.s;     }
                   | { $fn.getText().equals("max")            }? s_fn__max=fn__max         { expr = $s_fn__max.s;     }
                   | { $fn.getText().equals("count")          }? s_fn__count=fn__count     { expr = $s_fn__count.s;   }
                   | { $fn.getText().equals("str")            }? s_fn__str=fn__str         { expr = $s_fn__str.s;     }
                   | { $fn.getText().equals("regexp")         }? s_fn__regexp=fn__regexp   { expr = $s_fn__regexp.s;  }
                   | { $fn.getText().equals("percentile_agg") }? s_fn__pct_agg=fn__pct_agg { expr = $s_fn__pct_agg.s; }
                   )
                   | TAG_KW BRACE_OPEN_LIT                       s_fn__tag=fn__tag         { expr = $s_fn__tag.s;     }
                 ;

fn__rate         returns [ TimeSeriesMetricExpression s ]
                 @init{ Optional<Duration> interval = Optional.empty(); }
                 @after{ $s = new RateExpression($rate_arg.s, interval); }
                 : rate_arg=expression
                   (COMMA_LIT s_dur=duration { interval = Optional.of($s_dur.s); })?
                   BRACE_CLOSE_LIT
                 ;
fn__sum          returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new SumExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s)); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__avg          returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new AvgExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s)); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__min          returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new MinExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s)); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__max          returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new MaxExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s)); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__pct_agg      returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new PercentileAggregateExpression($pct.s.doubleValue(), $sel.s, Objects.requireNonNull($tag_agg.s)); }
                 : pct=number
                   COMMA_LIT sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__count        returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new CountExpression($sel.s, $tag_agg.s); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__tag          returns [ TimeSeriesMetricExpression s ]
                 @init{ Any2<TimeSeriesMetricExpression, GroupExpression> expr = null; }
                 @after{ $s = new TagValueExpression(expr, $s_tag.s); }
                 : ( s_grp=group{ expr = Any2.right($s_grp.s); }
                   | s_expr=expression{ expr = Any2.left($s_expr.s); }
                   )
                   COMMA_LIT s_tag=identifier BRACE_CLOSE_LIT
                 ;
fn__str          returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new StrConcatExpression($sel.s, $matcher.s); }
                 : sel=function_expression_arguments BRACE_CLOSE_LIT
                   matcher=by_match_clause
                 ;
fn__regexp       returns [ TimeSeriesMetricExpression s ]
                 @init{ String regexp = null; }
                 @after{ $s = new RegexpExpression($s_expr.s, regexp, $s_replacement.s); }
                 : s_expr=expression COMMA_LIT
                   ( s_regexp_str=quoted_string{ regexp = $s_regexp_str.s; }
                   | s_regexp_re=regex{ regexp = $s_regexp_re.s; }
                   ) COMMA_LIT
                   s_replacement=quoted_string
                   BRACE_CLOSE_LIT
                 ;

function_aggregate_argument returns [ Any2<MetricMatcher, TimeSeriesMetricExpression> s ]
                 @init{ Any2<MetricMatcher, TimeSeriesMetricExpression> result = null; }
                 @after{ $s = result; }
                 : s_match=metric_match
                   { result = Any2.left($s_match.s); }
                 | s_expr=expression
                   { result = Any2.right($s_expr.s); }
                 ;
function_aggregate_arguments returns [ List<Any2<MetricMatcher, TimeSeriesMetricExpression>> s = new ArrayList<>() ]
                 : sum_arg=function_aggregate_argument { $s.add($sum_arg.s); }
                   (COMMA_LIT sum_arg=function_aggregate_argument { $s.add($sum_arg.s); })*
                 ;
function_expression_arguments returns [ List<TimeSeriesMetricExpression> s = new ArrayList<>() ]
                 : s_expr=expression{ $s.add($s_expr.s); }
                   (COMMA_LIT s_expr=expression{ $s.add($s_expr.s); })*
                 ;


tag_aggregation_clause returns [ TagAggregationClause s ]
                 @init{ List<String> tags = new ArrayList<>(); }
                 : ( BY_KW BRACE_OPEN_LIT
                     ( s_i=identifier{ tags.add($s_i.s); }
                       (COMMA_LIT s_i=identifier{ tags.add($s_i.s); })*
                     )?
                     BRACE_CLOSE_LIT
                   )?
                   ( /* EMPTY */
                     { $s = (tags.isEmpty() ? TagAggregationClause.DEFAULT : TagAggregationClause.by(tags, false)); }
                   | KEEP_COMMON_KW
                     { $s = (tags.isEmpty() ? TagAggregationClause.KEEP_COMMON : TagAggregationClause.by(tags, true)); }
                   )
                 | WITHOUT_KW BRACE_OPEN_LIT
                   ( s_i=identifier{ tags.add($s_i.s); }
                     (COMMA_LIT s_i=identifier{ tags.add($s_i.s); })*
                   )
                   BRACE_CLOSE_LIT
                   { $s = TagAggregationClause.without(tags); }
                 ;
by_match_clause  returns [ TagMatchingClause s ]
                 @init{ List<String> tags = new ArrayList<>(); }
                 : /* EMPTY */
                   { $s = TagMatchingClause.DEFAULT; }
                 | BY_KW BRACE_OPEN_LIT
                   ( s_i=identifier{ tags.add($s_i.s); }
                     (COMMA_LIT s_i=identifier{ tags.add($s_i.s); })*
                   )?
                   BRACE_CLOSE_LIT
                   ( /* EMPTY */
                     { $s = TagMatchingClause.by(tags, false); }
                   | KEEP_COMMON_KW
                     { $s = TagMatchingClause.by(tags, true); }
                   )
                 ;
