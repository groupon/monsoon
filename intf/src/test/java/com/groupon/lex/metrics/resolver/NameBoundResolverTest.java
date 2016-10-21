package com.groupon.lex.metrics.resolver;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import java.util.stream.Collectors;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NameBoundResolverTest {
    @Test
    public void empty_is_empty() throws Exception {
        assertTrue(NameBoundResolver.EMPTY.isEmpty());
        assertEquals(EMPTY_LIST, NameBoundResolver.EMPTY.getKeys().collect(Collectors.toList()));
        assertEquals("", NameBoundResolver.EMPTY.configString());
        assertEquals(singletonList(NamedResolverMap.EMPTY), NameBoundResolver.EMPTY.resolve().collect(Collectors.toList()));
    }
}
