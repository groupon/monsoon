package com.groupon.lex.metrics;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Map;

public interface PushProcessor {
    public void accept(TimeSeriesCollection tsdata, Map<GroupName, Alert> alerts);
}
