package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.EMPTY_MAP;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

@Value
public class NameResolverSet {
    private final List<NameResolver> nameResolvers;

    public Stream<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolve() throws Exception {
        final List<List<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>>> sets = new ArrayList<>(nameResolvers.size());
        for (NameResolver nr : nameResolvers)
            sets.add(nr.resolve().collect(Collectors.toList()));

        return cartesianProduct(Stream.of(EMPTY_MAP), sets);
    }

    public List<Any2<Integer, String>> getKeys() {
        return nameResolvers.stream()
                .map(NameResolver::getNames)
                .map(NameResolver.Names::getNames)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public String configString() {
        if (isEmpty()) return "";
        return nameResolvers.stream()
                .map(NameResolver::toString)
                .collect(Collectors.joining(",\n", "{\n", "\n}"));
    }

    public boolean isEmpty() {
        return nameResolvers.stream()
                .allMatch(NameResolver::isEmpty);
    }

    private static <K, V> Stream<Map<K, V>> cartesianProduct(Stream<Map<K, V>> in, Iterable<? extends Collection<Map<K, V>>> sets) {
        final Iterator<? extends Collection<Map<K, V>>> setsIter = sets.iterator();

        while (!setsIter.hasNext()) {
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
