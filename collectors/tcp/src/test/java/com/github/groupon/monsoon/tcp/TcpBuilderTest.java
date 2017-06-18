package com.github.groupon.monsoon.tcp;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.Any3;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.function.BiConsumer;
import javax.servlet.http.HttpServlet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TcpBuilderTest {
    private static final SimpleGroupPath BASE_PATH = SimpleGroupPath.valueOf("foo", "bar");
    private static final NamedResolverMap ARGUMENTS = new NamedResolverMap(new HashMap<Any2<Integer, String>, Any3<Boolean, Integer, String>>() {
        {
            put(Any2.left(0), Any3.create3("baz"));
            put(Any2.right("host"), Any3.create3("localhost"));
            put(Any2.right("port"), Any3.create2(6000));
            put(Any2.right("metric"), Any3.create2(17));
        }
    });
    private static final Tags EXPECTED_TAGS = Tags.valueOf(new HashMap<String, MetricValue>() {
        {
            put("host", MetricValue.fromStrValue("localhost"));
            put("port", MetricValue.fromIntValue(6000));
            put("metric", MetricValue.fromIntValue(17));
        }
    });
    private static final GroupName EXPECTED_GROUP = GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar", "baz"), EXPECTED_TAGS);

    @Mock
    private BiConsumer<String, HttpServlet> er;
    @Mock
    private NameBoundResolver resolver;

    @Test
    public void makeTcpCollector() throws Exception {
        Mockito.when(resolver.getKeys()).thenAnswer((invocation) -> ARGUMENTS.getRawMap().keySet().stream());

        TcpBuilder builder = new TcpBuilder();
        builder.setAsPath(BASE_PATH);
        builder.setTagSet(resolver);
        GroupGenerator tcpCollector = builder.build(er).getCreateGenerator().create(ARGUMENTS);

        assertThat(tcpCollector, instanceOf(TcpCollector.class));
        assertThat(tcpCollector, hasProperty("dst", equalTo(new InetSocketAddress("localhost", 6000))));
        assertThat(tcpCollector, hasProperty("groupName", equalTo(EXPECTED_GROUP)));

        Mockito.verify(resolver, Mockito.atMost(1)).getKeys();
        Mockito.verifyNoMoreInteractions(resolver);
        Mockito.verifyZeroInteractions(er);
    }
}
