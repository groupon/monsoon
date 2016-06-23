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
grammar StringSubstitution;

@parser::header {
    import java.util.ArrayList;
    import java.util.List;
    import com.groupon.lex.metrics.lib.StringTemplate;
}

@lexer::header {
}

@lexer::members {
    public static enum State {
        INITIAL,
        BRACKET,
    }

    private State state_ = State.INITIAL;
}

expr             returns [ StringTemplate s ]
                 : s_elems=elements EOF
                   { $s = new StringTemplate($s_elems.s); }
                 ;

elements         returns [ List<StringTemplate.Element> s = new ArrayList<>(); ]
                 : (s0=element{ $s.add($s0.s); })*
                 ;

element          returns [ StringTemplate.Element s ]
                 : s_idx=index
                   { $s = $s_idx.s; }
                 | s_lit=literal
                   { $s = $s_lit.s; }
                 ;

index            returns [ StringTemplate.Element s ]
                 : s1=DOLLAR_INDEX
                   { $s = new StringTemplate.SubstituteElement(Integer.valueOf($s1.getText().substring(1))); }
                 | DOLLAR_CURLY_BRACKET_OPEN s2_idx=INDEX CURLY_BRACKET_CLOSE
                   { $s = new StringTemplate.SubstituteElement(Integer.valueOf($s2_idx.getText())); }
                 | DOLLAR_CURLY_BRACKET_OPEN s2_nam=IDENTIFIER CURLY_BRACKET_CLOSE
                   { $s = new StringTemplate.SubstituteNameElement($s2_nam.getText()); }
                 ;

literal          returns [ StringTemplate.LiteralElement s ]
                 : s0=literal_fragment
                   { $s = new StringTemplate.LiteralElement($s0.s); }
                 ;

literal_fragment returns [ String s ]
                 : s1=ANY
                   { $s = $s1.getText(); }
                 | s1=DOLLAR_DOLLAR
                   { $s = "$"; }
                 ;


DOLLAR_INDEX     : { state_ == State.INITIAL }? '$' ('0'..'9')+
                 ;
DOLLAR_CURLY_BRACKET_OPEN
                 : { state_ == State.INITIAL }? '${'
                   { state_ = State.BRACKET; }
                 ;
CURLY_BRACKET_CLOSE
                 : { state_ == State.BRACKET }? '}'
                   { state_ = State.INITIAL; }
                 ;
DOLLAR_DOLLAR    : { state_ == State.INITIAL }? '$$'
                 ;
INDEX            : { state_ == State.BRACKET }? ('0'..'9')+
                 ;
IDENTIFIER       : { state_ == State.BRACKET }? ('_'|'a'..'z'|'A'..'Z') ('_'|'a'..'z'|'A'..'Z'|'0'..'9')*
                 ;
ANY              : { state_ == State.INITIAL }? (~('$'))+
                 ;
