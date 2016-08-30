package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import static java.util.Collections.EMPTY_MAP;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface NameBoundResolver {
    public static final NameBoundResolver EMPTY = new NameBoundResolver() {
        @Override
        public Stream<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolve() { return Stream.of(EMPTY_MAP); }
        @Override
        public Stream<Any2<Integer, String>> getKeys() { return Stream.empty(); }
        @Override
        public boolean isEmpty() { return true; }
        @Override
        public String configString() { return ""; }
    };

    public Stream<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolve() throws Exception;
    public Stream<Any2<Integer, String>> getKeys();
    public boolean isEmpty();
    public String configString();

    /** @return a tag map from a resolved map. */
    public static Map<String, MetricValue> tagMap(Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> nrMap) {
        return nrMap.entrySet().stream()
                .map(entry -> {
                    final MetricValue tagValue = entry.getValue().mapCombine(MetricValue::fromBoolean, MetricValue::fromIntValue, MetricValue::fromStrValue);
                    return entry.getKey().getRight()
                            .map(tagName -> SimpleMapEntry.create(tagName, tagValue));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** @return an index-to-string map from a resolved map. */
    public static Map<Integer, String> indexToStringMap(Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> nrMap) {
        return nrMap.entrySet().stream()
                .map(entry -> {
                    final String tagValue = entry.getValue().mapCombine(String::valueOf, String::valueOf, String::valueOf);
                    return entry.getKey().getLeft()
                            .map(tagName -> SimpleMapEntry.create(tagName, tagValue));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
