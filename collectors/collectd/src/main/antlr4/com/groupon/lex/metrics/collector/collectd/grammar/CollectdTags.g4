grammar CollectdTags;

@parser::header {
    import java.util.Map;
    import java.util.HashMap;
    import com.groupon.lex.metrics.lib.Any2;
}

@lexer::header {
}

@lexer::members {
    public static enum State {
        INITIAL,  // Initial lexer state.
        QSTRING,  // Quoted string.
    }

    private State state_ = State.INITIAL;

    private boolean is_string_state_() {
        return state_ == State.QSTRING;
    }

    public static class CharEscapeException extends RuntimeException {
        CharEscapeException(String msg) { super(msg); }
        CharEscapeException(String msg, Exception t) { super(msg, t); }
    }
}

expr            returns [ Map<String, Any2<String, Number>> result ]
                : e=elements EOF
                  { $result = $e.result; }
                ;
elements        returns [ Map<String, Any2<String, Number>> result = new HashMap<>() ]
                : e=elem{ $result.put($e.key, $e.value); }
                  ( ',' e=elem{ $result.put($e.key, $e.value); } )*
                ;
elem            returns [ String key, Any2<String, Number> value ]
                : k=string[ false ]{ $key = $k.str; } '='
                  ( vn=number{ $value = Any2.right($vn.value); }
                  | vs=string[ true ]{ $value = Any2.left($vs.str); }
                  )
                ;

number          returns [ Number value ]
                : s1_dbl=fp_val
                  { $value = $s1_dbl.value; }
                | s1_int=int_val
                  { $value = $s1_int.value; }
                ;
fp_val          returns [ double value ]
                : s1=positive_fp_val
                  { $value = $s1.value; }
                | '-' s1=positive_fp_val
                  { $value = -$s1.value; }
                ;
positive_fp_val returns [ double value ]
                : s1=FP_DECIMAL
                  { $value = Double.parseDouble($s1.getText()); }
                | s1=FP_HEX
                  { $value = Double.parseDouble($s1.getText()); }
                ;
int_val         returns [ long value ]
                : s1=uint_val
                  { $value = $s1.value; }
                | '-' s1=uint_val
                  { $value = -$s1.value; }
                ;
uint_val        returns [ long value ]
                : s1=DIGITS
                  { $value = Long.parseLong($s1.getText()); }
                | s1=HEXDIGITS
                  { $value = Long.parseLong($s1.getText().substring(2), 16); }
                | s1=OCTDIGITS
                  { $value = Long.parseLong($s1.getText(), 8); }
                ;


string          [ boolean allow_comma ]
                returns [ String str ]
                : s1=quoted_string{ $str = $s1.str; }
                | s2=unquoted_string[ $allow_comma ]{ $str = $s2.str.toString(); }
                ;
quoted_string   returns [ String str ]
                : BEGIN_QUOTE s2=qstring_raw END_QUOTE
                  { $str = $s2.s.toString(); }
                ;
qstring_raw     returns [ StringBuilder s = new StringBuilder() ]
                : ( s1=RAW{ $s.append($s1.getText()); }
                  | s2=ESC_CHAR{ $s.append($s2.getText()); }
                  )*
                ;

unquoted_string [ boolean allow_comma ]
                returns [ StringBuilder str = new StringBuilder() ]
                : ( s1=UNQUOTED{ $str.append($s1.getText()); }
                  | { $allow_comma }? s2=','{ $str.append($s2.getText()); }
                  | '-'{ $str.append('-'); }
                  )*
                ;


/*
 * Number logic.
 */


DIGITS          : { state_ == State.INITIAL }? ('1'..'9') ('0'..'9')*
                ;
HEXDIGITS       : { state_ == State.INITIAL }? '0x' ('0'..'9'|'a'..'f'|'A'..'F')+
                ;
OCTDIGITS       : { state_ == State.INITIAL }? '0' ('0'..'7')*
                ;
FP_DECIMAL      : { state_ == State.INITIAL }? ('0'..'9')+ (('e'|'E') '-'? ('0'..'9')+)
                | { state_ == State.INITIAL }? ('0'..'9')* '.' ('0'..'9')+ (('e'|'E') ('0'..'9')+)?
                ;
FP_HEX          : { state_ == State.INITIAL }? '0x' ('0'..'9'|'a'..'f'|'A'..'F')+ ('.' ('0'..'9'|'a'..'f'|'A'..'F')*)
                | { state_ == State.INITIAL }? '0x' ('0'..'9'|'a'..'f'|'A'..'F')+ ('.' ('0'..'9'|'a'..'f'|'A'..'F')*)? (('p'|'P') '-'? ('0'..'9'|'a'..'f'|'A'..'F')+)
                ;


/*
 * String logic.
 *
 * Strings are enclosed in double quotes and may contain escape sequences.
 * Strings are sensitive to white space.
 */

BEGIN_QUOTE      : { state_ == State.INITIAL }? '\"'
                   { state_ = State.QSTRING; }
                 ;
END_QUOTE        : { state_ == State.QSTRING }? '\"'
                   { state_ = State.INITIAL; }
                 ;
ESC_CHAR         : { is_string_state_() }? '\\\\'
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
                         if (ch_int > 127) throw new CharEscapeException("Invalid octal escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException("octal escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\' ('0'..'7') ('0'..'7')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 8);
                         if (ch_int > 127) throw new CharEscapeException("Invalid octal escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException("octal escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\' ('0'..'7')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(1), 8);
                         if (ch_int > 127) throw new CharEscapeException("Invalid octal escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException("octal escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\x' ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(2), 16);
                         if (ch_int > 127) throw new CharEscapeException("Invalid hex escape");
                         setText(String.valueOf((char)ch_int));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException("hex escape: " + e.getMessage());
                     }
                   }
                 | { is_string_state_() }? '\\u' ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                         ('0'..'9' | 'a'..'f' | 'A'..'F')
                   {
                     try {
                         int ch_int = Integer.valueOf(getText().substring(2), 16);
                         if (ch_int > 0x10ffff) throw new CharEscapeException("Invalid hex escape");
                         setText(String.valueOf(Character.toChars(ch_int)));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException("utf-16 escape: " + e.getMessage());
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
                         int ch_int = Integer.valueOf(getText().substring(2), 16);
                         if (ch_int > 0x10ffff) throw new CharEscapeException("Invalid hex escape");
                         setText(String.valueOf(Character.toChars(ch_int)));
                     } catch (NumberFormatException e) {
                         throw new CharEscapeException("utf-32 escape: " + e.getMessage());
                     }
                   }
                 ;
RAW              : { is_string_state_() }? ~('\\'|'\"'|'\''|'/'|'\u0000'..'\u001f')+  /* backslash escape sequence handled by ESC_CHAR. */
                 | { is_string_state_() && state_ != State.QSTRING }? '\"'
                 ;


/*
 * A string without quotes.
 *
 * This is the last option, to make the lexer prefer all other options first.
 * If the unquoted string starts with a dash ('-'), UNQUOTED does not apply,
 * to allow string vs number disambiguation.
 */

UNQUOTED        : { state_ == State.INITIAL }? ~('"'|','|'='|'-') ~('"'|','|'=')*
                ;
