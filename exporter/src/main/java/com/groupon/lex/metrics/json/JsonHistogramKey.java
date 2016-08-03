package com.groupon.lex.metrics.json;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JsonHistogramKey {
    private double lo, high;
}
