package com.github.groupon.monsoon.tcp;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import static java.util.Collections.singletonMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TcpCollectorTest {
    private static final GroupName GROUP = GroupName.valueOf(SimpleGroupPath.valueOf("test"), Tags.valueOf(singletonMap("foo", MetricValue.fromStrValue("bar"))));
    private ServerSocketChannel dstSocket;
    private InetSocketAddress dstAddress;
    private ExecutorService acceptor;

    @Mock
    private Socket mockSocket;

    @Before
    public void setup() throws Exception {
        dstSocket = ServerSocketChannel.open();
        dstSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1);
        dstAddress = (InetSocketAddress) dstSocket.getLocalAddress();
        acceptor = Executors.newSingleThreadExecutor();
    }

    @After
    public void cleanup() throws Exception {
        dstSocket.close();
        acceptor.shutdown();
    }

    @Test(timeout = 20000)
    public void connectOk() throws Exception {
        CompletableFuture<SocketChannel> accepted = new CompletableFuture<>();
        acceptor.submit(() -> {
            try {
                accepted.complete(dstSocket.accept());
            } catch (Exception ex) {
                accepted.completeExceptionally(ex);
            }
        });

        final GroupGenerator.GroupCollection collected;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            collected = tcpCollector.getGroups();
        }

        assertTrue(collected.isSuccessful());
        assertThat(collected.getGroups(), contains(
                allOf(
                        hasProperty("name", equalTo(GROUP)),
                        hasProperty("metrics", arrayContainingInAnyOrder(
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("up"))),
                                        hasProperty("value", equalTo(MetricValue.TRUE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("latency")))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "msg"))),
                                        hasProperty("value", equalTo(MetricValue.EMPTY))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "type"))),
                                        hasProperty("value", equalTo(MetricValue.fromStrValue(TcpCollector.ConnectResult.OK.toString())))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "timed_out"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "no_route_to_host"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "port_unreachable"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "unknown_host"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "unknown_service"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "connect_failed"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "protocol_error"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "bind_failed"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE))),
                                allOf(hasProperty("name", equalTo(MetricName.valueOf("error", "io_error"))),
                                        hasProperty("value", equalTo(MetricValue.FALSE)))
                        ))
                )));
        assertTrue(accepted.get().isOpen());
        accepted.get().close();
    }

    @Test
    public void connectTimeout() throws Exception {
        Mockito.doThrow(new SocketTimeoutException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.TIMED_OUT));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectNoRouteToHost() throws Exception {
        Mockito.doThrow(new NoRouteToHostException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.NO_ROUTE_TO_HOST));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectPortUnreachable() throws Exception {
        Mockito.doThrow(new PortUnreachableException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.PORT_UNREACHABLE));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectUnknownHost() throws Exception {
        Mockito.doThrow(new UnknownHostException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.UNKNOWN_HOST));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectServiceError() throws Exception {
        Mockito.doThrow(new UnknownServiceException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.UNKNOWN_SERVICE));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectProtocolError() throws Exception {
        Mockito.doThrow(new ProtocolException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.PROTOCOL_ERROR));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectBindFailed() throws Exception {
        Mockito.doThrow(new BindException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.BIND_FAILED));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectFailed_withSocketException() throws Exception {
        Mockito.doThrow(new SocketException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.CONNECT_FAILED));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void connectFailed_withConnectException() throws Exception {
        Mockito.doThrow(new ConnectException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.CONNECT_FAILED));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }

    @Test
    public void anythingWeMissed() throws Exception {
        Mockito.doThrow(new IOException()).when(mockSocket).connect(Mockito.any(), Mockito.anyInt());

        final TcpCollector.ConnectDatum result;
        try (TcpCollector tcpCollector = new TcpCollector(dstAddress, GROUP)) {
            result = tcpCollector.tryConnect(mockSocket);
        }

        assertThat(result.getResult(), equalTo(TcpCollector.ConnectResult.IO_ERROR));
        Mockito.verify(mockSocket, times(1)).connect(Mockito.eq(dstAddress), Mockito.anyInt());
        Mockito.verifyNoMoreInteractions(mockSocket);
    }
}
