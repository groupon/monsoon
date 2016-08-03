package com.groupon.lex.metrics.json;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class Json {
    public static Object extractMetricValue(MetricValue value) {
        Boolean boolValue = value.getBoolValue();
        if (boolValue != null) return boolValue;
        Long intValue = value.getIntValue();
        if (intValue != null) return intValue;
        Double fltValue = value.getFltValue();
        if (fltValue != null) return fltValue;
        String strValue = value.getStrValue();
        if (strValue != null) return strValue;
        Histogram histValue = value.getHistValue();
        if (histValue != null) return extractHistogramObj(histValue);
        return null;
    }

    private static Map<JsonHistogramKey, Double> extractHistogramObj(Histogram histValue) {
        return histValue.stream()
                .map(range_count -> {
                    final double floor = range_count.getRange().getFloor();
                    final double ceil = range_count.getRange().getCeil();
                    return SimpleMapEntry.create(new JsonHistogramKey(floor, ceil), range_count.getCount());
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static void toJson(Gson gson, JsonWriter out, TimeSeriesCollection tsdata) throws IOException {
        out.beginObject();
        try {
            out.name("ts_msec").value(tsdata.getTimestamp().getMillis());
            out.name("ts").value(tsdata.getTimestamp().toString());
            out.name("data").beginObject();
            try {
                for (final TimeSeriesValue tsv : tsdata.getTSValues()) {
                    out.name(tsv.getGroup().configString().toString());
                    gson.toJson(new JsonTimeSeriesValue(tsv), JsonTimeSeriesValue.class, out);
                }
            } finally {
                out.endObject();
            }
        } finally {
            out.endObject();
        }
    }
}
