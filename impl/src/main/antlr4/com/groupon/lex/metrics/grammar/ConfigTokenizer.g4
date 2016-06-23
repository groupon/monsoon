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
lexer grammar ConfigTokenizer;
options {
    //backtrack = false;
    //memoize = true;
    //vocab = ConfigBnf;
    tokenVocab=ConfigBnf;
}

@header {
}

@members {
    public static enum State {
        INITIAL,  // Initial state.
        QSTRING,  // Quoted string.
        SQSTRING,  // Single-quote string.
        REGEX,  // Regular expression.
    }

    State state_ = State.INITIAL;

    private boolean is_string_state_() {
        return state_ == State.QSTRING ||
               state_ == State.SQSTRING ||
               state_ == State.REGEX;
    }
}


STAR_STAR        : { state_ == State.INITIAL }? '**'
                 ;
STAR             : { state_ == State.INITIAL }? '*'
                 ;
ENDSTATEMENT_KW  : { state_ == State.INITIAL }? ';'
                 ;
COMMENT          : { state_ == State.INITIAL }? '#' ~('\n')*
                   { skip(); }
                 ;
CURLYBRACKET_OPEN: { state_ == State.INITIAL }? '{'
                 ;
CURLYBRACKET_CLOSE
                 : { state_ == State.INITIAL }? '}'
                 ;
IMPORT_KW        : { state_ == State.INITIAL }? 'import'
                 ;
COLLECTORS_KW    : { state_ == State.INITIAL }? 'collectors'
                 ;
ALL_KW           : { state_ == State.INITIAL }? 'all'
                 ;
FROM_KW          : { state_ == State.INITIAL }? 'from'
                 ;
COLLECT_KW       : { state_ == State.INITIAL }? 'collect'
                 ;
CONSTANT_KW      : { state_ == State.INITIAL }? 'constant'
                 ;
ALERT_KW         : { state_ == State.INITIAL }? 'alert'
                 ;
IF_KW            : { state_ == State.INITIAL }? 'if'
                 ;
MESSAGE_KW       : { state_ == State.INITIAL }? 'message'
                 ;
FOR_KW           : { state_ == State.INITIAL }? 'for'
                 ;
MATCH_KW         : { state_ == State.INITIAL }? 'match'
                 ;
AS_KW            : { state_ == State.INITIAL }? 'as'
                 ;
ATTRIBUTES_KW    : { state_ == State.INITIAL }? 'attributes'
                 ;
WHERE_KW         : { state_ == State.INITIAL }? 'where'
                 ;
ALIAS_KW         : { state_ == State.INITIAL }? 'alias'
                 ;
TAG_KW           : { state_ == State.INITIAL }? 'tag'
                 ;
TRUE_KW          : { state_ == State.INITIAL }? 'true'
                 ;
FALSE_KW         : { state_ == State.INITIAL }? 'false'
                 ;
DEFINE_KW        : { state_ == State.INITIAL }? 'define'
                 ;
KEEP_COMMON_KW   : { state_ == State.INITIAL }? 'keep_common'
                 ;
BY_KW            : { state_ == State.INITIAL }? 'by'
                 ;
WITHOUT_KW       : { state_ == State.INITIAL }? 'without'
                 ;
WS               : { state_ == State.INITIAL }? (' '|'\n'|'\t')+
                   { skip(); }
                 ;
ID               : { state_ == State.INITIAL }? ('_'|'a'..'z'|'A'..'Z') ('_'|'a'..'z'|'A'..'Z'|'0'..'9')*
                 ;


FP_DECIMAL       : { state_ == State.INITIAL }? ('0'..'9')+ (('e'|'E') '-'? ('0'..'9')+)
                 | { state_ == State.INITIAL }? ('0'..'9')* '.' ('0'..'9')+ (('e'|'E') ('0'..'9')+)?
                 ;
FP_HEX           : { state_ == State.INITIAL }? '0x' ('0'..'9'|'a'..'f'|'A'..'F')+ ('.' ('0'..'9'|'a'..'f'|'A'..'F')*)
                 | { state_ == State.INITIAL }? '0x' ('0'..'9'|'a'..'f'|'A'..'F')+ ('.' ('0'..'9'|'a'..'f'|'A'..'F')*)? (('p'|'P') '-'? ('0'..'9'|'a'..'f'|'A'..'F')+)
                 ;
DIGITS           : { state_ == State.INITIAL }? ('1'..'9') ('0'..'9')*
                 ;
HEXDIGITS        : { state_ == State.INITIAL }? '0x' ('0'..'9'|'a'..'f'|'A'..'F')+
                 ;
OCTDIGITS        : { state_ == State.INITIAL }? '0' ('0'..'7')*
                 ;


/*
 * String logic.
 *
 * Strings are enclosed in double quotes and may contain escape sequences.
 * Strings are sensitive to white space.
 */

BEGIN_SQUOTE     : { state_ == State.INITIAL }? '\''
                   { state_ = State.SQSTRING; }
                 ;
BEGIN_QUOTE      : { state_ == State.INITIAL }? '\"'
                   { state_ = State.QSTRING; }
                 ;
BEGIN_REGEX      : { state_ == State.INITIAL }? '//'
                   { state_ = State.REGEX; }
                 ;
END_QUOTE        : { state_ == State.QSTRING }? '\"'
                   { state_ = State.INITIAL; }
                 | { state_ == State.SQSTRING }? '\''
                   { state_ = State.INITIAL; }
                 | { state_ == State.REGEX }? '//'
                   { state_ = State.INITIAL; }
                 ;
ESC_CHAR         : { is_string_state_() && state_ != State.REGEX }? '\\\\'
                   { setText("\\"); }
                 | { is_string_state_() }? '\\a'
                   { setText("\007"); }  /* unknown to java */
                 | { is_string_state_() }? '\\b'
                   { setText("\010"); }  /* unknown to java */
                 | { is_string_state_() }? '\\t'
                   { setText("\t"); }
                 | { is_string_state_() }? '\\n'
                   { setText("\n"); }
                 | { is_string_state_() }? '\\v'
                   { setText("\013"); }  /* unknown to java */
                 | { is_string_state_() }? '\\f'
                   { setText("\f"); }
                 | { is_string_state_() }? '\\r'
                   { setText("\r"); }
                 | { is_string_state_() }? '\\\''
                   { setText("\'"); }
                 | { is_string_state_() }? '\\\"'
                   { setText("\""); }
                 | { is_string_state_() }? '\\/'
                   { setText("/"); }
                 | { is_string_state_() }? '\\' ('0'..'7') ('0'..'7') ('0'..'7')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 8);
                         if (ch_int > 127) throw new CharEscapeException(this, "Invalid octal escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException(this, "octal escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\' ('0'..'7') ('0'..'7')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 8);
                         if (ch_int > 127) throw new CharEscapeException(this, "Invalid octal escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException(this, "octal escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\' ('0'..'7')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 8);
                         if (ch_int > 127) throw new CharEscapeException(this, "Invalid octal escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException(this, "octal escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\x' ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 16);
                         if (ch_int > 127) throw new CharEscapeException(this, "Invalid hex escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException(this, "hex escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\u' ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 16);
                         if (ch_int > 0x10ffff) throw new CharEscapeException(this, "Invalid hex escape");
                         setText(String.valueOf(Character.toChars(ch_int)));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException(this, "utf-16 escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\U' ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 16);
                         if (ch_int > 0x10ffff) throw new CharEscapeException(this, "Invalid hex escape");
                         setText(String.valueOf(Character.toChars(ch_int)));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException(this, "utf-32 escape: " + e.getMessage());
                     }
                   }
                 ;
RAW              : { is_string_state_() }? ~('\\'|'\"'|'\''|'/'|'\u0000'..'\u001f')+  /* backslash escape sequence handled by ESC_CHAR. */
                 | { is_string_state_() && state_ != State.QSTRING }? '\"'
                 | { is_string_state_() && state_ != State.SQSTRING }? '\''
                 | { is_string_state_() && state_ != State.REGEX }? '/'
                 | { state_ == State.REGEX }? '\\'
                 ;

LEFTSHIFT_KW     : { state_ == State.INITIAL }? '<<'
                 ;
RIGHTSHIFT_KW    : { state_ == State.INITIAL }? '>>'
                 ;
LE_KW            : { state_ == State.INITIAL }? '<='
                 ;
GE_KW            : { state_ == State.INITIAL }? '>='
                 ;
LT_KW            : { state_ == State.INITIAL }? '<'
                 ;
GT_KW            : { state_ == State.INITIAL }? '>'
                 ;
EQ_KW            : { state_ == State.INITIAL }? '='
                 ;
NEQ_KW           : { state_ == State.INITIAL }? '!='
                 ;
REGEX_MATCH_KW   : { state_ == State.INITIAL }? '=~'
                 ;
REGEX_NEGATE_KW  : { state_ == State.INITIAL }? '!~'
                 ;
LOGICAL_AND_KW   : { state_ == State.INITIAL }? '&&'
                 ;
LOGICAL_OR_KW    : { state_ == State.INITIAL }? '||'
                 ;


/*
 * Tokens for trivial character literals.
 */
COMMA_LIT        : { state_ == State.INITIAL }? ','
                 ;
DOT_LIT          : { state_ == State.INITIAL }? '.'
                 ;
PLUS_LIT         : { state_ == State.INITIAL }? '+'
                 ;
DASH_LIT         : { state_ == State.INITIAL }? '-'
                 ;
DOLLAR_LIT       : { state_ == State.INITIAL }? '$'
                 ;
BRACE_OPEN_LIT   : { state_ == State.INITIAL }? '('
                 ;
BRACE_CLOSE_LIT  : { state_ == State.INITIAL }? ')'
                 ;
SLASH_LIT        : { state_ == State.INITIAL }? '/'
                 ;
PERCENT_LIT      : { state_ == State.INITIAL }? '%'
                 ;
BANG_LIT         : { state_ == State.INITIAL }? '!'
                 ;
SQBRACE_OPEN_LIT : { state_ == State.INITIAL }? '['
                 ;
SQBRACE_CLOSE_LIT: { state_ == State.INITIAL }? ']'
                 ;
COLON_LIT        : { state_ == State.INITIAL }? ':'
                 ;
DOT_DOT_LIT      : { state_ == State.INITIAL }? '..'
                 ;
