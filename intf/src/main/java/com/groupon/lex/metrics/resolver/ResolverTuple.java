package com.groupon.lex.metrics.resolver;

import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.lib.Any3;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

/**
 * A tuple, as emitted by a resolver.
 */
@Value
@AllArgsConstructor
public class ResolverTuple {
    @NonNull
    private final List<Any3<Boolean, Integer, String>> fields;

    public ResolverTuple(Any3<Boolean, Integer, String>... values) {
        this(Arrays.asList(values));
    }

    public int getFieldsSize() { return getFields().size(); }

    @Override
    public String toString() {
        final Collector<CharSequence, ?, String> joiner;
        if (fields.size() == 1)
            joiner = Collectors.joining(", ");
        else
            joiner = Collectors.joining(", ", "(", ")");

        return fields.stream()
                .map(ResolverTuple::fieldString)
                .collect(joiner);
    }

    public static Any3<Boolean, Integer, String> newTupleElement(boolean b) { return Any3.create1(b); }
    public static Any3<Boolean, Integer, String> newTupleElement(int i) { return Any3.create2(i); }
    public static Any3<Boolean, Integer, String> newTupleElement(@NonNull String s) { return Any3.create3(s); }

    private static String fieldString(Any3<Boolean, Integer, String> field) {
        return field.mapCombine(String::valueOf, String::valueOf, s -> quotedString(s).toString());
    }
}
