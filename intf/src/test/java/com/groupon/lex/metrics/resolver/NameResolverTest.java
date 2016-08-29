package com.groupon.lex.metrics.resolver;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singletonList;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class NameResolverTest {
    @Test
    public void empty_is_empty() throws Exception {
        assertTrue(NameResolver.EMPTY.isEmpty());
        assertEquals(EMPTY_LIST, NameResolver.EMPTY.getKeys().collect(Collectors.toList()));
        assertEquals("", NameResolver.EMPTY.configString());
        assertEquals(singletonList(EMPTY_MAP), NameResolver.EMPTY.resolve().collect(Collectors.toList()));
    }
}
