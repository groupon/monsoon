package com.groupon.lex.metrics.lib;

import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class LazyMapTest {
    private Map<Integer, Integer> expected;
    private LazyMap<Integer, Integer> map;

    @Before
    public void setup() {
        expected = new HashMap<Integer, Integer>() {{
            put(1, 1);
            put(4, 16);
            put(9, 81);
        }};

        map = new LazyMap<>(x -> x * x, expected.keySet());
    }

    @Test
    public void entrySet() {
        assertEquals(expected.entrySet(), map.entrySet());
    }

    @Test
    public void keySet() {
        assertEquals(expected.keySet(), map.keySet());
    }

    @Test
    public void values() {
        System.out.println(new ArrayList<>(map.values()));
        assertThat(map.values(), containsInAnyOrder(1, 16, 81));
    }

    @Test
    public void sizeAndEmpty() {
        Map<Integer, Integer> empty = new LazyMap<>(Function.identity());

        assertTrue(empty.isEmpty());
        assertEquals(0, empty.size());

        assertFalse(map.isEmpty());
        assertEquals(expected.size(), map.size());
    }

    @Test
    public void get() {
        assertEquals(1, (int)map.get(1));
        assertEquals(16, (int)map.get(4));
        assertEquals(81, (int)map.get(9));

        assertNull(map.get(91));
        assertNull(map.get(null));
    }

    @Test
    public void put() {
        Integer expectedOld = expected.put(1, 17);
        Integer mapOld = map.put(1, 17);

        assertEquals(expectedOld, mapOld);
        assertEquals(expected, map);
    }

    @Test
    public void putAll() {
        expected.putAll(singletonMap(3, 3));
        map.putAll(singletonMap(3, 3));

        expected.putAll(singletonMap(1, 1));
        map.putAll(singletonMap(1, 1));

        assertEquals(expected, map);
    }

    @Test
    public void remove() {
        int expectedOld = expected.remove(4);
        int mapOld = map.remove(4);

        assertEquals(expectedOld, mapOld);
        assertEquals(expected, map);
    }

    @Test
    public void containsKey() {
        assertTrue(map.containsKey(4));
        assertFalse(map.containsKey(5));
    }

    @Test
    public void containsValue() {
        assertTrue(map.containsValue(16));
        assertFalse(map.containsValue(4));
    }

    @Test
    public void clear() {
        expected.clear();
        map.clear();

        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        assertEquals(expected, map);
    }

    @Test
    public void testHashCode() {
        assertEquals(expected.hashCode(), map.hashCode());
    }

    @Test
    public void equality() {
        assertTrue(map.equals(expected));
        assertFalse(map.equals(null));
        assertFalse(map.equals(new Object()));
        assertFalse(map.equals(singletonMap(1, 1)));
    }

    @Test
    public void entrySetHashCode() {
        assertEquals(expected.entrySet().hashCode(), map.entrySet().hashCode());
    }

    @Test
    public void entrySetEquality() {
        assertTrue(map.entrySet().equals(new HashSet<>(expected.entrySet())));
        assertFalse(map.entrySet().equals(EMPTY_SET));
        assertFalse(map.entrySet().equals(new Object()));
        assertFalse(map.entrySet().equals(null));
    }

    @Test
    public void entrySetAddAll() {
        Set<Map.Entry<Integer, Integer>> toAdd = singleton(SimpleMapEntry.create(3, 8));
        expected.put(3, 8);
        map.entrySet().addAll(toAdd);

        assertThat(map, hasEntry(3, 8));
        assertEquals(expected, map);
    }

    @Test
    public void entrySetContainsAll() {
        Set<Map.Entry<Integer, Integer>> absent = singleton(SimpleMapEntry.create(3, 8));
        Set<Map.Entry<Integer, Integer>> present = singleton(SimpleMapEntry.create(4, 16));
        List<Map.Entry<Integer, Integer>> present_and_absent = new ArrayList<Map.Entry<Integer, Integer>>() {{
            addAll(absent);
            addAll(present);
        }};

        assertTrue(map.entrySet().containsAll(present));
        assertFalse(map.entrySet().containsAll(absent));
        assertFalse(map.entrySet().containsAll(present_and_absent));
        assertFalse(map.entrySet().containsAll(singleton(new Object())));
        assertFalse(map.entrySet().containsAll(singleton(null)));
    }

    @Test
    public void entrySetRemoveAll() {
        Set<Map.Entry<Integer, Integer>> absent = singleton(SimpleMapEntry.create(3, 8));
        Set<Map.Entry<Integer, Integer>> present = singleton(SimpleMapEntry.create(4, 16));
        List<Map.Entry<Integer, Integer>> present_and_absent = new ArrayList<Map.Entry<Integer, Integer>>() {{
            addAll(singletonMap(9, 81).entrySet());  // present
            addAll(singletonMap(3, 3).entrySet());  // absent
        }};

        assertFalse(map.entrySet().removeAll(absent));
        assertEquals(expected, map);

        expected.entrySet().removeAll(present);
        assertTrue(map.entrySet().removeAll(present));
        assertEquals(expected, map);

        expected.entrySet().removeAll(present_and_absent);
        assertTrue(map.entrySet().removeAll(present_and_absent));
        assertEquals(expected, map);
    }

    @Test
    public void entrySetRetainAll() {
        List<Map.Entry<Integer, Integer>> present_and_absent = new ArrayList<Map.Entry<Integer, Integer>>() {{
            addAll(singletonMap(9, 81).entrySet());  // present
            addAll(singletonMap(3, 3).entrySet());  // absent
            addAll(singletonMap(4, 4).entrySet());  // absent
        }};

        assertTrue(map.entrySet().retainAll(present_and_absent));
        assertEquals(singletonMap(9, 81), map);
    }

    @Test
    public void entrySetClear() {
        map.entrySet().clear();

        assertTrue(map.isEmpty());
    }

    @Test
    public void entrySetSizeAndEmpty() {
        Map<Integer, Integer> empty = new LazyMap<>(Function.identity());

        assertEquals(3, map.entrySet().size());
        assertEquals(0, empty.size());

        assertFalse(map.entrySet().isEmpty());
        assertTrue(empty.isEmpty());
    }

    @Test
    public void entrySetToArray() {
        Object[] arr = map.entrySet().toArray();

        assertThat(arr, arrayContainingInAnyOrder(
                SimpleMapEntry.create(1,  1),
                SimpleMapEntry.create(4, 16),
                SimpleMapEntry.create(9, 81)));
    }

    @Test
    public void entrySetToArrayWithInitializer() {
        Map.Entry[] init_exact_size = new Map.Entry[3];
        Map.Entry[] init_too_small = new Map.Entry[2];
        Map.Entry[] init_empty = new Map.Entry[0];
        Map.Entry[] init_too_large = new Map.Entry[4];

        Map.Entry[] arr_exact_size = map.entrySet().toArray(init_exact_size);
        Map.Entry[] arr_too_small = map.entrySet().toArray(init_too_small);
        Map.Entry[] arr_empty = map.entrySet().toArray(init_empty);
        Map.Entry[] arr_too_large = map.entrySet().toArray(init_too_large);

        assertSame(init_exact_size, arr_exact_size);
        assertNotSame(init_too_small, arr_too_small);
        assertNotSame(init_empty, arr_empty);
        assertSame(init_too_large, arr_too_large);

        assertThat(arr_exact_size, arrayContainingInAnyOrder(
                SimpleMapEntry.create(1,  1),
                SimpleMapEntry.create(4, 16),
                SimpleMapEntry.create(9, 81)));
        assertThat(arr_too_small, arrayContainingInAnyOrder(
                SimpleMapEntry.create(1,  1),
                SimpleMapEntry.create(4, 16),
                SimpleMapEntry.create(9, 81)));
        assertThat(arr_empty, arrayContainingInAnyOrder(
                SimpleMapEntry.create(1,  1),
                SimpleMapEntry.create(4, 16),
                SimpleMapEntry.create(9, 81)));
        assertThat(Arrays.copyOf(arr_too_large, 3), arrayContainingInAnyOrder(
                SimpleMapEntry.create(1,  1),
                SimpleMapEntry.create(4, 16),
                SimpleMapEntry.create(9, 81)));
        assertNull(arr_too_large[3]);
    }

    @Test
    public void valuesSizeAndEmpty() {
        Map<?, ?> empty = new LazyMap<>(Function.identity());

        assertEquals(3, map.values().size());
        assertFalse(map.values().isEmpty());

        assertEquals(0, empty.values().size());
        assertTrue(empty.values().isEmpty());
    }

    @Test
    public void valuesToArray() {
        Object[] arr = map.values().toArray();

        assertThat(arr, arrayContainingInAnyOrder(1, 16, 81));
    }

    @Test
    public void valuesToArrayWithInitializer() {
        Number[] init_exact_size = new Number[3];
        Number[] init_too_small = new Number[2];
        Number[] init_empty = new Number[0];
        Number[] init_too_large = new Number[4];

        Number[] arr_exact_size = map.values().toArray(init_exact_size);
        Number[] arr_too_small = map.values().toArray(init_too_small);
        Number[] arr_empty = map.values().toArray(init_empty);
        Number[] arr_too_large = map.values().toArray(init_too_large);

        assertSame(init_exact_size, arr_exact_size);
        assertNotSame(init_too_small, arr_too_small);
        assertNotSame(init_empty, arr_empty);
        assertSame(init_too_large, arr_too_large);

        assertThat(arr_exact_size, arrayContainingInAnyOrder(1, 16, 81));
        assertThat(arr_too_small, arrayContainingInAnyOrder(1, 16, 81));
        assertThat(arr_empty, arrayContainingInAnyOrder(1, 16, 81));
        assertThat(Arrays.copyOf(arr_too_large, 3), arrayContainingInAnyOrder(1, 16, 81));
        assertNull(arr_too_large[3]);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void valuesAdd() {
        map.values().add(17);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void valuesAddAll() {
        map.values().addAll(EMPTY_LIST);
    }

    @Test
    public void valuesRemove() {
        expected.remove(4, 16);

        assertTrue(map.values().remove(16));
        assertFalse(map.values().remove(5));

        assertEquals(expected, map);
    }

    @Test
    public void valuesRemoveAll() {
        expected.remove(4, 16);
        assertTrue(map.values().removeAll(Arrays.asList(16, 17)));
        assertEquals(expected, map);

        assertFalse(map.values().removeAll(Arrays.asList(-1, -2)));
        assertEquals(expected, map);
    }

    @Test
    public void valuesContains() {
        assertTrue(map.values().contains(16));
        assertFalse(map.values().contains(17));
    }

    @Test
    public void valuesContainsAll() {
        assertTrue(map.values().containsAll(Arrays.asList(16, 81)));
        assertFalse(map.values().containsAll(Arrays.asList(16, 17)));
    }

    @Test
    public void valuesRetainAll() {
        expected.remove(9, 81);
        assertTrue(map.values().retainAll(Arrays.asList(1, 16)));
        assertEquals(expected, map);

        assertFalse(map.values().retainAll(Arrays.asList(1, 16, 17)));
        assertEquals(expected, map);
    }
}
