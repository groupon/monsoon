package com.groupon.lex.metrics.lib.sequence;

import java.io.Serializable;
import lombok.Value;

@Value
public class EqualRange implements Serializable {
    private final int begin, end;

    public boolean isEmpty() {
        return begin == end;
    }
}
