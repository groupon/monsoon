package com.groupon.lex.metrics.timeseries;

import java.util.stream.Stream;
import lombok.NonNull;

public interface ChainableExpressionLookBack extends ExpressionLookBack {
    @NonNull
    public ExpressionLookBack andThen(@NonNull ExpressionLookBack next);
    @NonNull
    public ExpressionLookBack andThen(@NonNull Stream<ExpressionLookBack> children);
}
