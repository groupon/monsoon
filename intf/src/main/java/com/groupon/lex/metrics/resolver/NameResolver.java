/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

/**
 *
 * @author ariane
 */
@Getter
@EqualsAndHashCode
public class NameResolver {
    private final Names names;
    private final Resolver resolver;

    public NameResolver(@NonNull Names names, @NonNull Resolver resolver) {
        if (names.getWidth() > resolver.getTupleWidth())
            throw new IllegalArgumentException("Insufficient tuple elements (" + resolver.getTupleWidth() + ") for names (at least " + names.getWidth() + " elements needed)");
        if (new HashSet<>(names.getNames()).size() != names.getNames().size())
            throw new IllegalArgumentException("Duplicate names");

        this.names = names;
        this.resolver = resolver;
    }

    public Stream<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolve() throws Exception {
        return resolver.getTuples().stream()
                .map(this::bindNames);
    }

    @Override
    public String toString() {
        return names + " = " + resolver;
    }

    public boolean isEmpty() {
        return names.isEmpty();
    }

    private Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> bindNames(ResolverTuple tuple) {
        final Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> result = new HashMap<>();
        final Iterator<Any3<Boolean, Integer, String>> fieldIter = tuple.getFields().iterator();
        final Iterator<Any2<Integer, String>> nameIter = names.getNames().iterator();

        while (nameIter.hasNext())
            result.put(nameIter.next(), fieldIter.next());
        return result;
    }

    @Value
    public static class Names {
        private final List<Any2<Integer, String>> names;

        public int getWidth() { return names.size(); }
        public boolean isEmpty() { return names.isEmpty(); }

        @Override
        public String toString() {
            final Collector<CharSequence, ?, String> nameJoiner;
            if (getWidth() == 1)
                nameJoiner = Collectors.joining(", ");
            else
                nameJoiner = Collectors.joining(", ", "(", ")");

            return names.stream()
                    .map(name -> name.mapCombine(String::valueOf, String::valueOf))
                    .collect(nameJoiner);
        }
    }
}
