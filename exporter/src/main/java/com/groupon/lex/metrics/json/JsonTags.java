package com.groupon.lex.metrics.json;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.groupon.lex.metrics.Tags;
import static com.groupon.lex.metrics.json.Json.extractMetricValue;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

class JsonTags extends HashMap<String, Object> {
    private static final Function<Tags, JsonTags> TAGS_CACHE = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((Tags in) -> new JsonTags(in)))::getUnchecked;

    public JsonTags() {}

    private JsonTags(Tags tags) {
        super(tags.stream()
                .collect(Collectors.toMap(tag_entry -> tag_entry.getKey(), tag_entry -> extractMetricValue(tag_entry.getValue()))));
    }

    public static JsonTags valueOf(Tags tags) {
        return TAGS_CACHE.apply(tags);
    }
}
