package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import static java.util.Objects.requireNonNull;
import lombok.NonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNull;

@Value
public class NameBoundResolverSet implements NameBoundResolver {
    private final List<NameBoundResolver> nameResolvers;

    public NameBoundResolverSet(@NonNull Collection<NameBoundResolver> nameResolvers) {
        nameResolvers.stream().forEach((nr) -> requireNonNull(nr, "null name resolver"));
        this.nameResolvers = unmodifiableList(new ArrayList<>(nameResolvers));
    }

    public NameBoundResolverSet(NameBoundResolver... nameResolvers) {
        this(Arrays.asList(nameResolvers));
    }

    @Override
    public Stream<NamedResolverMap> resolve() throws Exception {
        final List<List<NamedResolverMap>> sets = new ArrayList<>(nameResolvers.size());
        for (NameBoundResolver nr : nameResolvers)
            sets.add(nr.resolve().collect(Collectors.toList()));

        return cartesianProduct(sets);
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
                .map(s -> "    " + s) // Indentation
                .collect(Collectors.joining(",\n", "{\n", "\n}"));
    }

    @Override
    public boolean isEmpty() {
        return nameResolvers.stream()
                .allMatch(NameBoundResolver::isEmpty);
    }

    private static Stream<NamedResolverMap> cartesianProduct(Stream<NamedResolverMap> in, Iterable<? extends Collection<NamedResolverMap>> sets) {
        final Iterator<? extends Collection<NamedResolverMap>> setsIter = sets.iterator();

        while (setsIter.hasNext()) {
            final Collection<NamedResolverMap> head = setsIter.next();

            in = in
                    .flatMap(in_map -> {
                        return head.stream()
                                .map(headMap -> {
                                    Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> out_map = new HashMap<>();
                                    out_map.putAll(in_map.getRawMap());
                                    out_map.putAll(headMap.getRawMap());
                                    return new NamedResolverMap(out_map);
                                });
                    });
        }
        return in;
    }

    private static Stream<NamedResolverMap> cartesianProduct(Iterable<? extends Collection<NamedResolverMap>> sets) {
        return cartesianProduct(Stream.of(NamedResolverMap.EMPTY), sets);
    }

    @Override
    public void close() throws Exception {
        Exception thrown = null;
        for (NameBoundResolver resolver : nameResolvers) {
            try {
                resolver.close();
            } catch (Exception ex) {
                if (thrown == null)
                    thrown = ex;
                else
                    thrown.addSuppressed(ex);
            }
        }
        if (thrown != null) throw thrown;
    }
}
