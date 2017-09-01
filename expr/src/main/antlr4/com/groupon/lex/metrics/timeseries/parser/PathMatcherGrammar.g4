parser grammar PathMatcherGrammar;
options {
    tokenVocab=PathMatcherLexer;
}
import ExprBnf;

@header {
    import com.groupon.lex.metrics.PathMatcher;
}


expr             returns [ PathMatcher s ]
                 : s1=path_match EOF
                   { $s = $s1.s; }
                 ;
