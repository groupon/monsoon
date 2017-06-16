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
lexer grammar MonsoonExprLexer;

import MonsoonPrimitivesLexer;

TRUE_KW         : 'true'
                ;
FALSE_KW        : 'false'
                ;

KEEP_COMMON_KW  : 'keep_common'
                ;
BY_KW           : 'by'
                ;
WITHOUT_KW      : 'without'
                ;

LEFTSHIFT_KW    : '<<'
                ;
RIGHTSHIFT_KW   : '>>'
                ;
LE_KW           : '<='
                ;
GE_KW           : '>='
                ;
LT_KW           : '<'
                ;
GT_KW           : '>'
                ;
EQ_KW           : '='
                ;
NEQ_KW          : '!='
                ;
REGEX_MATCH_KW  : '=~'
                ;
REGEX_NEGATE_KW : '!~'
                ;
LOGICAL_AND_KW  : '&&'
                ;
LOGICAL_OR_KW   : '||'
                ;

/*
 * Tokens for trivial character literals.
 */
CURLYBRACKET_OPEN
                : '{'
                ;
CURLYBRACKET_CLOSE
                : '}'
                ;
STAR_STAR       : '**'
                ;
STAR            : '*'
                ;
COMMA_LIT       : ','
                ;
DOT_LIT         : '.'
                ;
PLUS_LIT        : '+'
                ;
DASH_LIT        : '-'
                ;
DOLLAR_LIT      : '$'
                ;
BRACE_OPEN_LIT  : '('
                ;
BRACE_CLOSE_LIT : ')'
                ;
SLASH_LIT       : '/'
                ;
PERCENT_LIT     : '%'
                ;
BANG_LIT        : '!'
                ;
SQBRACE_OPEN_LIT: '['
                ;
SQBRACE_CLOSE_LIT
                : ']'
                ;
COLON_LIT       : ':'
                ;
DOT_DOT_LIT     : '..'
                ;

/*
 * Special case TAG_KW, since "tag" is also a keyword.
 */
TAG_KW          : 'tag'
                ;
