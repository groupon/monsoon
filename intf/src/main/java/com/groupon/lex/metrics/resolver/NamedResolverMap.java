package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.ConfigSupport;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.unmodifiableMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

/**
 * A map type to abstract away the complexities of accessing resolver values.
 *
 * @author ariane
 */
@AllArgsConstructor
@EqualsAndHashCode
public class NamedResolverMap {
    public static NamedResolverMap EMPTY = new NamedResolverMap(EMPTY_MAP);

    @NonNull
    private final Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> data;

    /**
     * Return the boolean value indicated by the given numeric key.
     *
     * @param key The key of the value to return.
     * @param dfl The default value to return, if the key is absent.
     * @return The boolean value stored under the given key, or dfl.
     * @throws IllegalArgumentException if the value is present, but not a
     * boolean.
     */
    public boolean getBooleanOrDefault(int key, boolean dfl) {
        Any3<Boolean, Integer, String> value = data.getOrDefault(Any2.<Integer, String>left(key), Any3.<Boolean, Integer, String>create1(dfl));
        return value.get1().orElseThrow(() -> new IllegalArgumentException("expected boolean argument for param " + key));
    }

    /**
     * Return the boolean value indicated by the given string key.
     *
     * @param key The key of the value to return.
     * @param dfl The default value to return, if the key is absent.
     * @return The boolean value stored under the given key, or dfl.
     * @throws IllegalArgumentException if the value is present, but not a
     * boolean.
     */
    public boolean getBooleanOrDefault(@NonNull String key, boolean dfl) {
        Any3<Boolean, Integer, String> value = data.getOrDefault(Any2.<Integer, String>right(key), Any3.<Boolean, Integer, String>create1(dfl));
        return value.get1().orElseThrow(() -> new IllegalArgumentException("expected boolean argument for param " + key));
    }

    /**
     * Return the integer value indicated by the given numeric key.
     *
     * @param key The key of the value to return.
     * @param dfl The default value to return, if the key is absent.
     * @return The integer value stored under the given key, or dfl.
     * @throws IllegalArgumentException if the value is present, but not an
     * integer.
     */
    public int getIntegerOrDefault(int key, int dfl) {
        Any3<Boolean, Integer, String> value = data.getOrDefault(Any2.<Integer, String>left(key), Any3.<Boolean, Integer, String>create2(dfl));
        return value.get2().orElseThrow(() -> new IllegalArgumentException("expected integer argument for param " + key));
    }

    /**
     * Return the integer value indicated by the given string key.
     *
     * @param key The key of the value to return.
     * @param dfl The default value to return, if the key is absent.
     * @return The integer value stored under the given key, or dfl.
     * @throws IllegalArgumentException if the value is present, but not an
     * integer.
     */
    public int getIntegerOrDefault(@NonNull String key, int dfl) {
        Any3<Boolean, Integer, String> value = data.getOrDefault(Any2.<Integer, String>right(key), Any3.<Boolean, Integer, String>create2(dfl));
        return value.get2().orElseThrow(() -> new IllegalArgumentException("expected integer argument for param " + key));
    }

    /**
     * Return the string value indicated by the given numeric key.
     *
     * @param key The key of the value to return.
     * @param dfl The default value to return, if the key is absent.
     * @return The string value stored under the given key, or dfl.
     * @throws IllegalArgumentException if the value is present, but not a
     * string.
     */
    public String getStringOrDefault(int key, @NonNull String dfl) {
        Any3<Boolean, Integer, String> value = data.getOrDefault(Any2.<Integer, String>left(key), Any3.<Boolean, Integer, String>create3(dfl));
        return value.get3().orElseThrow(() -> new IllegalArgumentException("expected string argument for param " + key));
    }

    /**
     * Return the string value indicated by the given string key.
     *
     * @param key The key of the value to return.
     * @param dfl The default value to return, if the key is absent.
     * @return The string value stored under the given key, or dfl.
     * @throws IllegalArgumentException if the value is present, but not a
     * string.
     */
    public String getStringOrDefault(@NonNull String key, @NonNull String dfl) {
        Any3<Boolean, Integer, String> value = data.getOrDefault(Any2.<Integer, String>right(key), Any3.<Boolean, Integer, String>create3(dfl));
        return value.get3().orElseThrow(() -> new IllegalArgumentException("expected string argument for param " + key));
    }

    /**
     * Return the boolean value indicated by the given numeric key.
     *
     * @param key The key of the value to return.
     * @return The boolean value stored under the given key.
     * @throws IllegalArgumentException if the value is absent or not a boolean.
     */
    public boolean getBoolean(int key) {
        Any3<Boolean, Integer, String> value = data.get(Any2.<Integer, String>left(key));
        if (value == null)
            throw new IllegalArgumentException("missing argument for param " + key);
        return value.get1().orElseThrow(() -> new IllegalArgumentException("expected boolean argument for param " + key));
    }

    /**
     * Return the boolean value indicated by the given string key.
     *
     * @param key The key of the value to return.
     * @return The boolean value stored under the given key.
     * @throws IllegalArgumentException if the value is absent or not a boolean.
     */
    public boolean getBoolean(@NonNull String key) {
        Any3<Boolean, Integer, String> value = data.get(Any2.<Integer, String>right(key));
        if (value == null)
            throw new IllegalArgumentException("missing argument for param " + key);
        return value.get1().orElseThrow(() -> new IllegalArgumentException("expected boolean argument for param " + key));
    }

    /**
     * Return the integer value indicated by the given numeric key.
     *
     * @param key The key of the value to return.
     * @return The integer value stored under the given key.
     * @throws IllegalArgumentException if the value is absent or not an
     * integer.
     */
    public int getInteger(int key) {
        Any3<Boolean, Integer, String> value = data.get(Any2.<Integer, String>left(key));
        if (value == null)
            throw new IllegalArgumentException("missing argument for param " + key);
        return value.get2().orElseThrow(() -> new IllegalArgumentException("expected integer argument for param " + key));
    }

    /**
     * Return the integer value indicated by the given string key.
     *
     * @param key The key of the value to return.
     * @return The integer value stored under the given key.
     * @throws IllegalArgumentException if the value is absent or not an
     * integer.
     */
    public int getInteger(@NonNull String key) {
        Any3<Boolean, Integer, String> value = data.get(Any2.<Integer, String>right(key));
        if (value == null)
            throw new IllegalArgumentException("missing argument for param " + key);
        return value.get2().orElseThrow(() -> new IllegalArgumentException("expected integer argument for param " + key));
    }

    /**
     * Return the string value indicated by the given numeric key.
     *
     * @param key The key of the value to return.
     * @return The string value stored under the given key.
     * @throws IllegalArgumentException if the value is absent or not a string.
     */
    public String getString(int key) {
        Any3<Boolean, Integer, String> value = data.get(Any2.<Integer, String>left(key));
        if (value == null)
            throw new IllegalArgumentException("missing argument for param " + key);
        return value.get3().orElseThrow(() -> new IllegalArgumentException("expected string argument for param " + key));
    }

    /**
     * Return the string value indicated by the given string key.
     *
     * @param key The key of the value to return.
     * @return The string value stored under the given key.
     * @throws IllegalArgumentException if the value is absent or not a string.
     */
    public String getString(@NonNull String key) {
        Any3<Boolean, Integer, String> value = data.get(Any2.<Integer, String>right(key));
        if (value == null)
            throw new IllegalArgumentException("missing argument for param " + key);
        return value.get3().orElseThrow(() -> new IllegalArgumentException("expected string argument for param " + key));
    }

    /**
     * The numeric set of keys held by the resolver map.
     *
     * @return the numberic set of keys held by the resolver map.
     */
    public Set<Integer> intKeySet() {
        return data.keySet().stream()
                .map(Any2::getLeft)
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toSet());
    }

    /**
     * The string set of keys held by the resolver map.
     *
     * @return the string set of keys held by the resolver map.
     */
    public Set<String> stringKeySet() {
        return data.keySet().stream()
                .map(Any2::getRight)
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toSet());
    }

    /**
     * Create a GroupName from this NamedResolverMap.
     *
     * @param prefixPath Additional path elements to put in front of the
     * returned path.
     * @param extraTags Additional tags to put in the tag set. The extraTags
     * argument will override any values present in the NamedResolverMap.
     * @return A group name derived from this NamedResolverMap and the supplied
     * arguments.
     */
    public GroupName getGroupName(@NonNull List<String> prefixPath, @NonNull Map<String, MetricValue> extraTags) {
        final Stream<String> suffixPath = data.entrySet().stream()
                .filter(entry -> entry.getKey().getLeft().isPresent()) // Only retain int keys.
                .sorted(Comparator.comparing(entry -> entry.getKey().getLeft().get())) // Sort by int key.
                .map(Map.Entry::getValue)
                .map(value -> value.mapCombine(b -> b.toString(), i -> i.toString(), Function.identity()));

        final SimpleGroupPath path = SimpleGroupPath.valueOf(Stream.concat(prefixPath.stream(), suffixPath).collect(Collectors.toList()));
        final Map<String, MetricValue> tags = data.entrySet().stream()
                .filter(entry -> entry.getKey().getRight().isPresent()) // Only retain string keys.
                .collect(Collectors.toMap(entry -> entry.getKey().getRight().get(), entry -> entry.getValue().mapCombine(MetricValue::fromBoolean, MetricValue::fromIntValue, MetricValue::fromStrValue)));
        tags.putAll(extraTags);
        return GroupName.valueOf(path, tags);
    }

    /**
     * Create a GroupName from this NamedResolverMap.
     *
     * @param prefixPath Additional path elements to put in front of the
     * returned path.
     * @return A group name derived from this NamedResolverMap and the supplied
     * arguments.
     */
    public GroupName getGroupName(@NonNull List<String> prefixPath) {
        return NamedResolverMap.this.getGroupName(prefixPath, EMPTY_MAP);
    }

    /**
     * Create a GroupName from this NamedResolverMap.
     *
     * @param extraTags Additional tags to put in the tag set. The extraTags
     * argument will override any values present in the NamedResolverMap.
     * @return A group name derived from this NamedResolverMap and the supplied
     * arguments.
     */
    public GroupName getGroupName(@NonNull Map<String, MetricValue> extraTags) {
        return NamedResolverMap.this.getGroupName(EMPTY_LIST, extraTags);
    }

    /**
     * Create a GroupName from this NamedResolverMap.
     *
     * @return A group name derived from this NamedResolverMap.
     */
    public GroupName getGroupName() {
        return NamedResolverMap.this.getGroupName(EMPTY_LIST, EMPTY_MAP);
    }

    /**
     * Get the underlying map of data.
     *
     * @return The raw map underlying this NamedResolverMap.
     */
    public Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> getRawMap() {
        return unmodifiableMap(data);
    }

    /**
     * Get the underlying map, but with all values converted to string.
     *
     * @return The raw map with every value converted to a string.
     */
    public Map<Any2<Integer, String>, String> getStringMap() {
        return data.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().mapCombine(Object::toString, Object::toString, Object::toString)));
    }

    @Override
    public String toString() {
        Stream<String> intSet = data.entrySet().stream()
                .filter(entry -> entry.getKey().getLeft().isPresent())
                .sorted(Comparator.comparing(entry -> entry.getKey().getLeft().get()))
                .map(entry -> entry.getKey().getLeft().get() + "=" + entry.getValue().mapCombine(b -> b.toString(), i -> i.toString(), s -> ConfigSupport.quotedString(s)));
        Stream<String> strSet = data.entrySet().stream()
                .filter(entry -> entry.getKey().getRight().isPresent())
                .sorted(Comparator.comparing(entry -> entry.getKey().getRight().get()))
                .map(entry -> ConfigSupport.maybeQuoteIdentifier(entry.getKey().getRight().get()) + "=" + entry.getValue().mapCombine(b -> b.toString(), i -> i.toString(), s -> ConfigSupport.quotedString(s)));
        return Stream.concat(intSet, strSet)
                .collect(Collectors.joining(", ", "NamedResolverMap{", "}"));
    }
}
