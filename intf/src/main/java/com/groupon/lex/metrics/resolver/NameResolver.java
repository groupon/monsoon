package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.util.Map;
import java.util.stream.Stream;

public interface NameResolver {
    public static final NameResolver EMPTY = new NameResolver() {
        @Override
        public Stream<Map<Any2<Integer, String>, Any3<Boolean, Integer, String>>> resolve() { return Stream.empty(); }
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
}
