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
parser grammar ExprBnf;

@header{
    import com.groupon.lex.metrics.Histogram;
    import com.groupon.lex.metrics.MetricMatcher;
    import com.groupon.lex.metrics.PathMatcher;
    import com.groupon.lex.metrics.expression.GroupExpression;
    import com.groupon.lex.metrics.expression.IdentifierGroupExpression;
    import com.groupon.lex.metrics.expression.LiteralGroupExpression;
    import com.groupon.lex.metrics.lib.Any2;
    import com.groupon.lex.metrics.lib.TriFunction;
    import com.groupon.lex.metrics.timeseries.TagAggregationClause;
    import com.groupon.lex.metrics.timeseries.TagMatchingClause;
    import com.groupon.lex.metrics.timeseries.expression.AvgExpression;
    import com.groupon.lex.metrics.timeseries.expression.CountExpression;
    import com.groupon.lex.metrics.timeseries.expression.IdentifierMetricSelector;
    import com.groupon.lex.metrics.timeseries.expression.MaxExpression;
    import com.groupon.lex.metrics.timeseries.expression.MinExpression;
    import com.groupon.lex.metrics.timeseries.expression.NameExpression;
    import com.groupon.lex.metrics.timeseries.expression.PercentileAggregateExpression;
    import com.groupon.lex.metrics.timeseries.expression.RateExpression;
    import com.groupon.lex.metrics.timeseries.expression.RegexpExpression;
    import com.groupon.lex.metrics.timeseries.expression.StrConcatExpression;
    import com.groupon.lex.metrics.timeseries.expression.SumExpression;
    import com.groupon.lex.metrics.timeseries.expression.TagValueExpression;
    import com.groupon.lex.metrics.timeseries.expression.Util;
    import com.groupon.lex.metrics.timeseries.expression.UtilX;
    import com.groupon.lex.metrics.transformers.IdentifierNameResolver;
    import com.groupon.lex.metrics.transformers.LiteralNameResolver;
    import com.groupon.lex.metrics.transformers.NameResolver;
    import com.groupon.lex.metrics.timeseries.parser.Scope;
    import com.groupon.lex.metrics.timeseries.parser.MutableScope;

    import java.util.ArrayDeque;
    import java.util.ArrayList;
    import java.util.Collections;
    import java.util.Deque;
    import java.util.List;
    import java.util.Objects;
    import java.util.Optional;
    import java.util.function.BiFunction;
    import java.util.function.Function;
    import org.joda.time.Duration;
}

@members{
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
}

identifier       returns [ String s ]
                 @init{
                   String text;
                 }
                 @after{
                   $s = text;
                 }
                 : s1_tok=ID
                   { text = $s1_tok.getText(); }
                 | s1_str=SQSTRING
                   { text = $s1_str.getText(); }
                 ;
name             returns [ NameResolver s ]
                 @init{
                   NameResolver resolver;
                 }
                 @after{
                   $s = resolver;
                 }
                 : s0=name_elem{ resolver = $s0.s; }
                   ( DOT_LIT s_=name_elem
                     { resolver = NameResolver.combine(resolver, $s_.s); }
                   )*
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
                 : s1=QSTRING
                   { $s = $s1.getText(); }
                 ;
quoted_identifier returns [ String s = "" ]
                 : s1=SQSTRING
                   { $s = $s1.getText(); }
                 ;
regex            returns [ String s = "" ]
                 : s1=REGEX
                   { $s = $s1.getText(); }
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
                 : fn=ID
                   ( { $fn.getText().equals("rate")           }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__rate=fn__rate[ $tdelta.s ]       { expr = $s_fn__rate.s;    }
                   | { $fn.getText().equals("sum")            }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__sum=fn__sum[ $tdelta.s ]         { expr = $s_fn__sum.s;     }
                   | { $fn.getText().equals("avg")            }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__avg=fn__avg[ $tdelta.s ]         { expr = $s_fn__avg.s;     }
                   | { $fn.getText().equals("min")            }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__min=fn__min[ $tdelta.s ]         { expr = $s_fn__min.s;     }
                   | { $fn.getText().equals("max")            }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__max=fn__max[ $tdelta.s ]         { expr = $s_fn__max.s;     }
                   | { $fn.getText().equals("count")          }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__count=fn__count[ $tdelta.s ]     { expr = $s_fn__count.s;   }
                   | { $fn.getText().equals("str")            }?                              BRACE_OPEN_LIT s_fn__str=fn__str                      { expr = $s_fn__str.s;     }
                   | { $fn.getText().equals("regexp")         }?                              BRACE_OPEN_LIT s_fn__regexp=fn__regexp                { expr = $s_fn__regexp.s;  }
                   | { $fn.getText().equals("percentile_agg") }? tdelta=function_opt_duration BRACE_OPEN_LIT s_fn__pct_agg=fn__pct_agg[ $tdelta.s ] { expr = $s_fn__pct_agg.s; }
                   | { $fn.getText().equals("name")           }?                              BRACE_OPEN_LIT s_fn__name=fn__name                    { expr = $s_fn__name.s;    }
                   )
                   | TAG_KW                                                                   BRACE_OPEN_LIT s_fn__tag=fn__tag                      { expr = $s_fn__tag.s;     }
                 ;
function_opt_duration returns [ Optional<Duration> s = Optional.empty() ]
                 : ( SQBRACE_OPEN_LIT d=duration SQBRACE_CLOSE_LIT
                     { $s = Optional.of($d.s); }
                   )?
                 ;

fn__rate         [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new RateExpression($rate_arg.s, $interval); }
                 : rate_arg=expression
                   ( { !$interval.isPresent() }?
                     COMMA_LIT s_dur=duration
                     { $interval = Optional.of($s_dur.s); }
                   )?
                   BRACE_CLOSE_LIT
                 ;
fn__sum          [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new SumExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s), $interval); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__avg          [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new AvgExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s), $interval); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__min          [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new MinExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s), $interval); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__max          [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new MaxExpression(Objects.requireNonNull($sel.s), Objects.requireNonNull($tag_agg.s), $interval); }
                 : sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__pct_agg      [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new PercentileAggregateExpression($pct.s.doubleValue(), $sel.s, Objects.requireNonNull($tag_agg.s), $interval); }
                 : pct=number
                   COMMA_LIT sel=function_aggregate_arguments BRACE_CLOSE_LIT tag_agg=tag_aggregation_clause
                 ;
fn__count        [ Optional<Duration> interval ] returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new CountExpression($sel.s, $tag_agg.s, $interval); }
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
fn__name         returns [ TimeSeriesMetricExpression s ]
                 @after{ $s = new NameExpression($s_name.s); }
                 : s_name=name
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
