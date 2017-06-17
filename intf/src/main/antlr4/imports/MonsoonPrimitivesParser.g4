/*
 * Copyright (c) 2016, 2017, Groupon, Inc.
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
lexer grammar MonsoonPrimitivesLexer;

@header{
    import com.groupon.lex.metrics.Histogram;

    import java.util.ArrayList;
    import java.util.List;
    import java.util.Objects;
    import java.util.function.Function;
    import org.joda.time.Duration;
}

identifier      returns [ String s ]
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
 * Duration.
 * A duration is 1 or more numbers, each with a unit attached.
 * Example:  "1d 4h" denotes 1 day + 4 hours.
 */

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

