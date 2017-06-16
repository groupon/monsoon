parser grammar Expression;
options {
    tokenVocab=ExpressionLexer;
}
import ExprBnf;

@header {
    import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
}


expr             returns [ TimeSeriesMetricExpression s ]
                 : s1=expression EOF
                   { $s = $s1.s; }
                 ;
