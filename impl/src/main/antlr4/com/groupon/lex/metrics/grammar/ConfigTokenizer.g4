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
    //tokenVocab=ConfigBnf;
}

import MonsoonExprLexer;

@header {
    import java.util.regex.Matcher;
    import java.util.regex.Pattern;
}


ENDSTATEMENT_KW  : ';'
                 ;
COMMENT          : '#' ~('\n')*
                   { skip(); }
                 ;
IMPORT_KW        : 'import'
                 ;
COLLECTORS_KW    : 'collectors'
                 ;
ALL_KW           : 'all'
                 ;
FROM_KW          : 'from'
                 ;
COLLECT_KW       : 'collect'
                 ;
CONSTANT_KW      : 'constant'
                 ;
ALERT_KW         : 'alert'
                 ;
IF_KW            : 'if'
                 ;
MESSAGE_KW       : 'message'
                 ;
FOR_KW           : 'for'
                 ;
MATCH_KW         : 'match'
                 ;
AS_KW            : 'as'
                 ;
ATTRIBUTES_KW    : 'attributes'
                 ;
WHERE_KW         : 'where'
                 ;
ALIAS_KW         : 'alias'
                 ;
DEFINE_KW        : 'define'
                 ;
