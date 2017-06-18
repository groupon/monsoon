package com.groupon.lex.metrics;

import com.groupon.lex.metrics.api.ApiServer;
import com.groupon.lex.metrics.config.Configuration;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.instanceOf;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PipelineBuilderTest {
    @Mock
    private EndpointRegistration api;
    @Mock
    private CollectHistory history;
    @Mock
    private PipelineBuilder.PushProcessorSupplier pps;
    @Mock
    private PushProcessor pp;

    @Before
    public void setup() throws Exception {
        when(pps.build(Mockito.any())).thenReturn(pp);
        when(history.streamReversed()).thenThrow(new UnsupportedOperationException());
        when(history.stream()).thenAnswer((invocation) -> Stream.empty());
        when(history.stream(Mockito.any(DateTime.class), Mockito.any(DateTime.class))).thenAnswer((invocation) -> Stream.empty());
        when(history.getEnd()).thenReturn(DateTime.now(DateTimeZone.UTC));
    }

    @Test
    public void build() throws Exception {
        try (final PullProcessorPipeline pipeline = new PipelineBuilder(Configuration.DEFAULT)
                .build()) {
            assertThat(pipeline.getMetricRegistry().getApi(), instanceOf(ApiServer.class));
        }
    }

    @Test
    public void buildWithApi() throws Exception {
        try (final PullProcessorPipeline pipeline = new PipelineBuilder(Configuration.DEFAULT)
                .withApi(api)
                .build()) {
            assertSame(api, pipeline.getMetricRegistry().getApi());
        }
    }

    @Test
    public void buildWithHistoryAndApi() throws Exception {
        try (final PushProcessorPipeline pipeline = new PipelineBuilder(Configuration.DEFAULT)
                .withApi(api)
                .withHistory(history)
                .build(pps)) {
            assertSame(history, pipeline.getMetricRegistry().getHistory().get());
            assertSame(api, pipeline.getMetricRegistry().getApi());
        }

        verify(api, times(1)).setHistory(Mockito.same(history));
    }

    @Test
    public void buildWithHistory() throws Exception {
        try (final PushProcessorPipeline pipeline = new PipelineBuilder(Configuration.DEFAULT)
                .withHistory(history)
                .withCollectIntervalSeconds(15)
                .build(pps)) {
            assertSame(history, pipeline.getMetricRegistry().getHistory().get());
            assertThat(pipeline.getMetricRegistry().getApi(), instanceOf(ApiServer.class));
            assertEquals(15, pipeline.getIntervalSeconds());
        }
    }
}
