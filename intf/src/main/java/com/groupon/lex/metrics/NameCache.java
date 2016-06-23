package com.groupon.lex.metrics;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 * A shared cache for types that tend to not vary much and get instantiated an
 * awful lot.
 *
 * It allows multiple values that are the same, to be collated into a shared
 * instance.  This class does not aim to improve constructor performance.
 */
public final class NameCache {
    private NameCache() {}
    public static final NameCache singleton = new NameCache();

    @Value
    private static class GroupNameArgs {
        SimpleGroupPath path;
        Tags tags;
    }

    private final Function<List<String>, MetricName> metric_name_cache_ = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((List<String> list) -> new MetricName(list)))::getUnchecked;
    private final Function<List<String>, SimpleGroupPath> simplegrouppath_cache_ = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((List<String> pathelems) -> new SimpleGroupPath(pathelems)))::getUnchecked;
    private final Function<Map<String, MetricValue>, Tags> tags_cache_ = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((Map<String, MetricValue> in) -> new Tags(in)))::getUnchecked;
    private final Function<GroupNameArgs, GroupName> groupname_cache_ = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((GroupNameArgs in) -> new GroupName(in.getPath(), in.getTags())))::getUnchecked;

    /** MetricName constructor. */
    public MetricName newMetricName(String... input) {
        return newMetricName(Arrays.asList(input));
    }
    /** MetricName constructor. */
    public MetricName newMetricName(List<String> input) {
        return metric_name_cache_.apply(input);
    }

    /** SimpleGroupPath constructor. */
    public SimpleGroupPath newSimpleGroupPath(String... input) {
        return newSimpleGroupPath(Arrays.asList(input));
    }
    /** SimpleGroupPath constructor. */
    public SimpleGroupPath newSimpleGroupPath(List<String> input) {
        return simplegrouppath_cache_.apply(input);
    }

    /** Tag constructor. */
    public Tags newTags(Stream<Map.Entry<String, MetricValue>> input) {
        return newTags(input.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
    /** Tag constructor. */
    public Tags newTags(Map<String, MetricValue> input) {
        if (input.isEmpty()) return Tags.EMPTY;
        return tags_cache_.apply(input);
    }

    /** GroupName constructor. */
    public GroupName newGroupName(String... path) {
        return newGroupName(new GroupNameArgs(newSimpleGroupPath(path), Tags.EMPTY));
    }
    /** GroupName constructor. */
    public GroupName newGroupName(SimpleGroupPath path) {
        return newGroupName(new GroupNameArgs(path, Tags.EMPTY));
    }
    /** GroupName constructor. */
    public GroupName newGroupName(SimpleGroupPath path, Tags tags) {
        return newGroupName(new GroupNameArgs(path, tags));
    }
    /** GroupName constructor. */
    public GroupName newGroupName(SimpleGroupPath path, Map<String, MetricValue> tags) {
        return newGroupName(new GroupNameArgs(path, newTags(tags)));
    }
    /** GroupName constructor. */
    public GroupName newGroupName(SimpleGroupPath path, Stream<Map.Entry<String, MetricValue>> tags) {
        return newGroupName(new GroupNameArgs(path, newTags(tags)));
    }
    /** GroupName constructor. */
    private GroupName newGroupName(GroupNameArgs args) {
        return groupname_cache_.apply(args);
    }
}
