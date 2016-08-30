package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import static java.util.Objects.requireNonNull;

@Value
public class NameBoundResolverSet implements NameBoundResolver {
    private final List<NameBoundResolver> nameResolvers;

    public NameBoundResolverSet(Collection<NameBoundResolver> nameResolvers) {
        for (NameBoundResolver nr : nameResolvers)
            requireNonNull(nr, "null name resolver");
        this.nameResolvers = unmodifiableList(new ArrayList<>(nameResolvers));
    }

    public NameBoundResolverSet(NameBoundResolver... nameResolvers) {
        this(Arrays.asList(nameResolvers));
    }

    @Override
    public Stream<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolve() throws Exception {
        final List<List<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>>> sets = new ArrayList<>(nameResolvers.size());
        for (NameBoundResolver nr : nameResolvers)
            sets.add(nr.resolve().collect(Collectors.toList()));

        return cartesianProduct(Stream.of(EMPTY_MAP), sets);
    }

    @Override
    public Stream<Any2<Integer, String>> getKeys() {
        return nameResolvers.stream()
                .flatMap(NameBoundResolver::getKeys);
    }

    @Override
    public String configString() {
        if (isEmpty()) return "{}";
        return nameResolvers.stream()
                .map(NameBoundResolver::configString)
                .map(s -> "    " + s)  // Indentation
                .collect(Collectors.joining(",\n", "{\n", "\n}"));
    }

    @Override
    public boolean isEmpty() {
        return nameResolvers.stream()
                .allMatch(NameBoundResolver::isEmpty);
    }

    private static <K, V> Stream<Map<K, V>> cartesianProduct(Stream<Map<K, V>> in, Iterable<? extends Collection<Map<K, V>>> sets) {
        final Iterator<? extends Collection<Map<K, V>>> setsIter = sets.iterator();

        while (setsIter.hasNext()) {
            final Collection<Map<K, V>> head = setsIter.next();

            in = in
                    .flatMap(in_map -> {
                        return head.stream()
                                .map(headMap -> {
                                    Map<K, V> out_map = new HashMap<>();
                                    out_map.putAll(in_map);
                                    out_map.putAll(headMap);
                                    return out_map;
                                });
                    });
        }
        return in;
    }
}
