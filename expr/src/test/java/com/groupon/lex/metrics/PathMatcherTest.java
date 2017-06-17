package com.groupon.lex.metrics;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class PathMatcherTest {
    @Test
    public void literalTest() {
        final PathMatcher pm = new PathMatcher(new PathMatcher.LiteralNameMatch("foo"), new PathMatcher.LiteralNameMatch("bar"));

        assertTrue(pm.match(Arrays.asList("foo", "bar")));
        assertFalse(pm.match(Arrays.asList("foo", "baz")));
        assertFalse(pm.match(Arrays.asList("foo")));
        assertFalse(pm.match(Arrays.asList("foo", "bar", "baz")));
    }

    @Test
    public void regexTest() {
        final PathMatcher pm = new PathMatcher(new PathMatcher.LiteralNameMatch("s"), new PathMatcher.RegexMatch("^[ab]+$"));

        assertTrue(pm.match(Arrays.asList("s", "abba")));
        assertFalse(pm.match(Arrays.asList("s", "other")));
        assertFalse(pm.match(Arrays.asList("s", "")));
        assertFalse(pm.match(Arrays.asList("s", "abbaZZ")));
    }

    @Test
    public void wildcardTest() {
        final PathMatcher pm = new PathMatcher(new PathMatcher.LiteralNameMatch("s"), new PathMatcher.WildcardMatch());

        assertTrue(pm.match(Arrays.asList("s", "abba")));
        assertTrue(pm.match(Arrays.asList("s", "zzz")));
        assertFalse(pm.match(Arrays.asList("s")));
        assertFalse(pm.match(Arrays.asList("s", "abba", "e")));
    }

    @Test
    public void doubleWildcardTest() {
        final PathMatcher pm = new PathMatcher(new PathMatcher.LiteralNameMatch("s"), new PathMatcher.DoubleWildcardMatch());

        assertTrue(pm.match(Arrays.asList("s", "abba")));
        assertTrue(pm.match(Arrays.asList("s", "zzz")));
        assertTrue(pm.match(Arrays.asList("s")));
        assertTrue(pm.match(Arrays.asList("s", "abba", "e")));
    }

    @Test
    public void doubleWildcardBacktrackingTest() {
        final PathMatcher pm = new PathMatcher(
                new PathMatcher.LiteralNameMatch("1"),
                new PathMatcher.DoubleWildcardMatch(),
                new PathMatcher.LiteralNameMatch("2"),
                new PathMatcher.DoubleWildcardMatch(),
                new PathMatcher.LiteralNameMatch("3"));

        assertTrue(pm.match(Arrays.asList("1", "a", "b", "2", "c", "d", "3")));
        assertTrue(pm.match(Arrays.asList("1", "2", "3")));
        assertFalse(pm.match(Arrays.asList("1", "a", "b", "2", "c", "d")));
        assertTrue(pm.match(Arrays.asList("1", "2", "2", "2", "2", "2", "3")));
    }

    @Test
    public void configStringTest() {
        final PathMatcher pm = new PathMatcher(
                new PathMatcher.LiteralNameMatch("1"),
                new PathMatcher.WildcardMatch(),
                new PathMatcher.RegexMatch("^[a-z]+$"),
                new PathMatcher.DoubleWildcardMatch(),
                new PathMatcher.LiteralNameMatch("last"));

        assertEquals("'1'.*.//^[a-z]+$//.**.last", pm.configString().toString());
    }

    @Test
    public void toStringTest() {
        final PathMatcher pm = new PathMatcher(
                new PathMatcher.LiteralNameMatch("1"),
                new PathMatcher.WildcardMatch(),
                new PathMatcher.RegexMatch("^[a-z]+$"),
                new PathMatcher.DoubleWildcardMatch(),
                new PathMatcher.LiteralNameMatch("last"));

        assertEquals("PathMatcher:'1'.*.//^[a-z]+$//.**.last", pm.toString());
    }

    @Test
    public void equalityTest() {
        final PathMatcher pm1 = new PathMatcher(
                new PathMatcher.LiteralNameMatch("1"),
                new PathMatcher.WildcardMatch(),
                new PathMatcher.RegexMatch("^[a-z]+$"),
                new PathMatcher.DoubleWildcardMatch(),
                new PathMatcher.LiteralNameMatch("last"));

        assertTrue(pm1.equals(pm1));

        final PathMatcher pm2 = new PathMatcher(
                new PathMatcher.LiteralNameMatch("1"),
                new PathMatcher.WildcardMatch(),
                new PathMatcher.RegexMatch("^[a-z]+$"),
                new PathMatcher.DoubleWildcardMatch(),
                new PathMatcher.LiteralNameMatch("last"));

        assertEquals(pm1.hashCode(), pm2.hashCode());
        assertTrue(pm1.equals(pm2));
        assertTrue(pm2.equals(pm1));
    }
}
