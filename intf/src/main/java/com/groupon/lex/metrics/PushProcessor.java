package com.groupon.lex.metrics;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Map;

public interface PushProcessor extends AutoCloseable {
    public void accept(TimeSeriesCollection tsdata, Map<GroupName, Alert> alerts) throws Exception;
    @Override
    public default void close() throws Exception {}
}
