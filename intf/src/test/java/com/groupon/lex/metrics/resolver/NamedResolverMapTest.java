package com.groupon.lex.metrics.resolver;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public class NamedResolverMapTest {
    private NamedResolverMap map;

    @Before
    public void setup() {
        Map<Any2<Integer, String>, Any3<Boolean, Integer, String>> rawMap = new HashMap<>();

        rawMap.put(Any2.left(5), Any3.create1(true));
        rawMap.put(Any2.right("five"), Any3.create1(true));
        rawMap.put(Any2.left(6), Any3.create1(false));
        rawMap.put(Any2.right("six"), Any3.create1(false));
        // 7 and "seven" intentionally omitted.
        rawMap.put(Any2.left(8), Any3.create2(18));
        rawMap.put(Any2.right("eight"), Any3.create2(18));
        rawMap.put(Any2.left(9), Any3.create3("foobarium"));
        rawMap.put(Any2.right("nine"), Any3.create3("foobarium"));

        map = new NamedResolverMap(rawMap);
    }

    @Test
    public void five() {
        assertTrue(map.getBoolean(5));
        assertTrue(map.getBoolean("five"));
        assertTrue(map.getBooleanOrDefault(5, false));
        assertTrue(map.getBooleanOrDefault("five", false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fiveIntIntFails() {
        map.getInteger(5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fiveStrIntFails() {
        map.getInteger("five");
    }

    @Test(expected = IllegalArgumentException.class)
    public void fiveIntStrFails() {
        map.getInteger(5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fiveStrStrFails() {
        map.getInteger("five");
    }

    @Test
    public void six() {
        assertFalse(map.getBoolean(6));
        assertFalse(map.getBoolean("six"));
        assertFalse(map.getBooleanOrDefault(6, true));
        assertFalse(map.getBooleanOrDefault("six", true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void sevenIntBoolFails() {
        map.getBoolean(7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sevenStrBoolFails() {
        map.getBoolean("seven");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sevenIntIntFails() {
        map.getInteger(7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sevenStrIntFails() {
        map.getInteger("seven");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sevenIntStrFails() {
        map.getString(7);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sevenStrStrFails() {
        map.getString("seven");
    }

    @Test
    public void seven() {
        assertFalse(map.getBooleanOrDefault(7, false));
        assertTrue(map.getBooleanOrDefault(7, true));
        assertFalse(map.getBooleanOrDefault("seven", false));
        assertTrue(map.getBooleanOrDefault("seven", true));

        assertEquals(17, map.getIntegerOrDefault(7, 17));
        assertEquals(17, map.getIntegerOrDefault("seven", 17));

        assertEquals("foo", map.getStringOrDefault(7, "foo"));
        assertEquals("bar", map.getStringOrDefault("seven", "bar"));
    }

    @Test
    public void eight() {
        assertEquals(18, map.getIntegerOrDefault(8, 100));
        assertEquals(18, map.getIntegerOrDefault("eight", 100));
        assertEquals(18, map.getInteger(8));
        assertEquals(18, map.getInteger("eight"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void eightIntBoolFails() {
        map.getBooleanOrDefault(8, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void eightStrBoolFails() {
        map.getBooleanOrDefault("eight", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void eightIntStrFails() {
        map.getStringOrDefault(8, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void eightStrStrFails() {
        map.getStringOrDefault("eight", "");
    }

    @Test
    public void nine() {
        assertEquals("foobarium", map.getStringOrDefault(9, "blablabla"));
        assertEquals("foobarium", map.getStringOrDefault("nine", "blablabla"));
        assertEquals("foobarium", map.getString(9));
        assertEquals("foobarium", map.getString("nine"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nineIntBoolFails() {
        map.getBooleanOrDefault(9, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nineStrBoolFails() {
        map.getBooleanOrDefault("nine", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nineIntIntFails() {
        map.getIntegerOrDefault(9, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nineStrIntFails() {
        map.getIntegerOrDefault("nine", 1);
    }

    @Test
    public void keySets() {
        assertEquals(new HashSet<>(Arrays.asList(5, 6, 8, 9)), map.intKeySet());
        assertEquals(new HashSet<>(Arrays.asList("five", "six", "eight", "nine")), map.stringKeySet());
    }

    @Test
    public void stringMap() {
        Map<Any2<Integer, String>, String> expected = new HashMap<>();
        expected.put(Any2.left(5), "true");
        expected.put(Any2.right("five"), "true");
        expected.put(Any2.left(6), "false");
        expected.put(Any2.right("six"), "false");
        expected.put(Any2.left(8), "18");
        expected.put(Any2.right("eight"), "18");
        expected.put(Any2.left(9), "foobarium");
        expected.put(Any2.right("nine"), "foobarium");

        assertEquals(expected, map.getStringMap());
    }

    @Test
    public void groupName() {
        SimpleGroupPath expectedPath = SimpleGroupPath.valueOf("true", "false", "18", "foobarium");
        Map<String, MetricValue> expectedTags = new HashMap<>();
        expectedTags.put("five", MetricValue.TRUE);
        expectedTags.put("six", MetricValue.FALSE);
        expectedTags.put("eight", MetricValue.fromIntValue(18));
        expectedTags.put("nine", MetricValue.fromStrValue("foobarium"));

        assertEquals(GroupName.valueOf(expectedPath, expectedTags), map.getGroupName());
    }

    @Test
    public void groupNameWithPrefix() {
        SimpleGroupPath expectedPath = SimpleGroupPath.valueOf("pre", "fix", "true", "false", "18", "foobarium");
        Map<String, MetricValue> expectedTags = new HashMap<>();
        expectedTags.put("five", MetricValue.TRUE);
        expectedTags.put("six", MetricValue.FALSE);
        expectedTags.put("eight", MetricValue.fromIntValue(18));
        expectedTags.put("nine", MetricValue.fromStrValue("foobarium"));

        assertEquals(GroupName.valueOf(expectedPath, expectedTags), map.getGroupName(Arrays.asList("pre", "fix")));
    }

    @Test
    public void groupNameWithTags() {
        SimpleGroupPath expectedPath = SimpleGroupPath.valueOf("true", "false", "18", "foobarium");
        Map<String, MetricValue> expectedTags = new HashMap<>();
        expectedTags.put("five", MetricValue.TRUE);
        expectedTags.put("six", MetricValue.FALSE);
        expectedTags.put("eight", MetricValue.fromIntValue(18));
        expectedTags.put("nine", MetricValue.fromDblValue(3.14));
        expectedTags.put("bla bla", MetricValue.fromStrValue("chocoladevla"));

        assertEquals(GroupName.valueOf(expectedPath, expectedTags),
                map.getGroupName(new HashMap<String, MetricValue>() {
                    {
                        put("nine", MetricValue.fromDblValue(3.14));
                        put("bla bla", MetricValue.fromStrValue("chocoladevla"));
                    }
                }));
    }

    @Test
    public void groupNameWithPrefixAndTags() {
        SimpleGroupPath expectedPath = SimpleGroupPath.valueOf("pre", "fix", "true", "false", "18", "foobarium");
        Map<String, MetricValue> expectedTags = new HashMap<>();
        expectedTags.put("five", MetricValue.TRUE);
        expectedTags.put("six", MetricValue.FALSE);
        expectedTags.put("eight", MetricValue.fromIntValue(18));
        expectedTags.put("nine", MetricValue.fromDblValue(3.14));
        expectedTags.put("bla bla", MetricValue.fromStrValue("chocoladevla"));

        assertEquals(GroupName.valueOf(expectedPath, expectedTags),
                map.getGroupName(Arrays.asList("pre", "fix"), new HashMap<String, MetricValue>() {
                    {
                        put("nine", MetricValue.fromDblValue(3.14));
                        put("bla bla", MetricValue.fromStrValue("chocoladevla"));
                    }
                }));
    }
}
