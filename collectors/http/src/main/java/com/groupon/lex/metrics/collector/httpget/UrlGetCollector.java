/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.collector.httpget;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.SimpleMetric;
import com.groupon.lex.metrics.SimpleMetricGroup;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.singleton;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.joda.time.Instant;
import org.joda.time.Interval;

/**
 *
 * @author ariane
 */
public class UrlGetCollector implements GroupGenerator {
    private static final Logger LOG = Logger.getLogger(UrlGetCollector.class.getName());
    public static final int TIMEOUT_SECONDS = 10;
    public static final int OVERALL_TIMEOUT_SECONDS = 30;
    @Getter
    private final SimpleGroupPath baseGroupName;
    @Getter
    private final UrlPattern patterns;
    private final RequestConfig request_config_ = RequestConfig.custom()
            .setConnectionRequestTimeout(TIMEOUT_SECONDS * 1000)
            .setConnectTimeout(TIMEOUT_SECONDS * 1000)
            .setSocketTimeout(TIMEOUT_SECONDS * 1000)
            .build();
    private static Reference<GCCloseable<CloseableHttpAsyncClient>> http_client_ = new WeakReference<>(null);  // Retrieve it via get_http_client_().
    private GCCloseable<CloseableHttpAsyncClient> httpClient = get_http_client_();

    private static final MetricName MN_STATUS_CODE = MetricName.valueOf("status", "code");
    private static final MetricName MN_STATUS_LINE = MetricName.valueOf("status", "line");
    private static final MetricName MN_PROTOCOL_NAME = MetricName.valueOf("protocol", "name");
    private static final MetricName MN_PROTOCOL_MAJOR = MetricName.valueOf("protocol", "major");
    private static final MetricName MN_PROTOCOL_MINOR = MetricName.valueOf("protocol", "minor");
    private static final MetricName MN_LATENCY = MetricName.valueOf("latency");
    private static final MetricName MN_LOCALE_COUNTRY = MetricName.valueOf("locale", "country");
    private static final MetricName MN_LOCALE_LANGUAGE = MetricName.valueOf("locale", "language");
    private static final MetricName MN_CONTENT_CHUNKED = MetricName.valueOf("content", "chunked");
    private static final MetricName MN_CONTENT_LENGTH = MetricName.valueOf("content", "length");
    private static final MetricName MN_CONTENT_TYPE = MetricName.valueOf("content", "type");
    private static final MetricName MN_CONTENT_CHARSET = MetricName.valueOf("content", "charset");
    private static final MetricName MN_CONTENT_MIMETYPE = MetricName.valueOf("content", "mimetype");
    private static final MetricName MN_UP = MetricName.valueOf("up");

    private static synchronized GCCloseable<CloseableHttpAsyncClient> get_http_client_() {
        GCCloseable<CloseableHttpAsyncClient> result = http_client_.get();
        if (result != null && !result.get().isRunning())
            result = null;  // Reactor appears to spontaneously shut down.
        if (result == null) {
            result = new GCCloseable<>(HttpAsyncClientBuilder.create()
                    .useSystemProperties()
                    .build());
            result.get().start();
            http_client_ = new WeakReference<>(result);
        }
        return result;
    }

    private synchronized GCCloseable<CloseableHttpAsyncClient> httpClient() {
        GCCloseable<CloseableHttpAsyncClient> result = httpClient;
        if (!result.get().isRunning()) // Reactor appears to spontaneously shut down.
            httpClient = result = get_http_client_();
        return result;
    }

    public UrlGetCollector(SimpleGroupPath base_group_name, UrlPattern patterns) {
        baseGroupName = requireNonNull(base_group_name);
        this.patterns = requireNonNull(patterns);
    }

    private long get_len_from_stream_(ByteCountingInputStream in) throws IOException {
        final byte[] buf = new byte[256];

        for (;;) {
            final int avail = in.available();
            if (avail > 0) {
                final long skipped = in.skip(avail);
                if (skipped > 0) continue;
            }
            final int rdlen = in.read(buf);
            if (rdlen == -1) break;  // GUARD
        }

        return in.getBytesRead();
    }

    protected Stream<Metric> processStream(Header[] response_headers, String contentType, Optional<Charset> charset, InputStream in) throws IOException {
        return Stream.empty();
    }

    /**
     * Generate metrics from response.
     */
    private Stream<Metric> do_response_(Instant begin_ts, HttpResponse response) throws IOException {
        final Instant end_ts = Instant.now();  // Record end of operation.

        try (final InputStream entity_stream = response.getEntity().getContent()) {
            final ByteCountingInputStream byte_counting_stream = new ByteCountingInputStream(entity_stream, false);

            /* Collect all headers. */
            final Stream<SimpleMetric> hdr_stream = Arrays.stream(response.getAllHeaders())
                    .map((Header hdr) -> {
                        final MetricName metric_name = MetricName.valueOf("header", hdr.getName());
                        try {
                            long intHeader = Long.valueOf(hdr.getValue());
                            return new SimpleMetric(metric_name, MetricValue.fromIntValue(intHeader));
                        } catch (NumberFormatException ex) {
                            /* SKIP */
                        }
                        try {
                            double dblHeader = Double.valueOf(hdr.getValue());
                            return new SimpleMetric(metric_name, MetricValue.fromDblValue(dblHeader));
                        } catch (NumberFormatException ex) {
                            /* SKIP */
                        }
                        return new SimpleMetric(metric_name, MetricValue.fromStrValue(hdr.getValue()));
                    });
            /* Collect individual metrics. */
            final Stream<SimpleMetric> response_stream = Stream.of(
                    new SimpleMetric(MN_STATUS_CODE,
                            Optional.ofNullable(response.getStatusLine().getStatusCode())
                            .map(MetricValue::fromIntValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_STATUS_LINE,
                            Optional.ofNullable(response.getStatusLine().getReasonPhrase())
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_PROTOCOL_NAME,
                            Optional.ofNullable(response.getProtocolVersion().getProtocol())
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_PROTOCOL_MAJOR,
                            Optional.ofNullable(response.getProtocolVersion().getMajor())
                            .map(MetricValue::fromIntValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_PROTOCOL_MINOR,
                            Optional.ofNullable(response.getProtocolVersion().getMinor())
                            .map(MetricValue::fromIntValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_LATENCY,
                            MetricValue.fromIntValue(new Interval(begin_ts, end_ts).toDurationMillis())),
                    new SimpleMetric(MN_LOCALE_COUNTRY,
                            Optional.ofNullable(response.getLocale().getCountry())
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_LOCALE_LANGUAGE,
                            Optional.ofNullable(response.getLocale().getLanguage())
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY)));
            /* Collect stream processing data. */
            final Stream<Metric> stream_result = processStream(response.getAllHeaders(), ContentType.get(response.getEntity()).getMimeType(), Optional.ofNullable(ContentType.get(response.getEntity()).getCharset()), byte_counting_stream);

            final Optional<Long> content_len = Optional.ofNullable(Optional.of(response.getEntity().getContentLength())
                    .filter(len -> len > 0) // 0 may indicate no data, or perhaps no known length.
                    .orElseGet(() -> {
                        try {
                            return get_len_from_stream_(byte_counting_stream);
                        } catch (IOException ex) {
                            return null;
                        }
                    }));
            final ContentType content_type = ContentType.get(response.getEntity());
            final Stream<SimpleMetric> content_stream = Stream.of(
                    new SimpleMetric(MN_CONTENT_CHUNKED,
                            Optional.ofNullable(response.getEntity().isChunked())
                            .map(MetricValue::fromBoolean)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_CONTENT_LENGTH, content_len.map(MetricValue::fromIntValue).orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_CONTENT_TYPE,
                            Optional.ofNullable(response.getEntity().getContentType())
                            .map(Header::getValue)
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_CONTENT_CHARSET,
                            Optional.ofNullable(content_type.getCharset())
                            .map(Charset::name)
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY)),
                    new SimpleMetric(MN_CONTENT_MIMETYPE,
                            Optional.ofNullable(content_type.getMimeType())
                            .map(MetricValue::fromStrValue)
                            .orElse(MetricValue.EMPTY))
            );
            final Stream<SimpleMetric> up_stream = Stream.of(
                    new SimpleMetric(MN_UP, MetricValue.TRUE)
            );

            return Stream.of(hdr_stream, response_stream, content_stream, stream_result, up_stream).flatMap(Function.identity());
        }
    }

    /**
     * Takes a response and fills in the future for the metric group.
     */
    private class HttpResponseConsumer implements FutureCallback<HttpResponse> {
        private final Instant begin_ts = Instant.now();  // Record begin of operation.
        private final CompletableFuture<Stream<Metric>> output_;
        private final GroupName args_;
        private final String url_;
        /**
         * Objects that need to be kept referenced until the request completes.
         */
        private final Collection<Object> keep_live_;

        public HttpResponseConsumer(CompletableFuture<Stream<Metric>> output, GroupName args, String url, Object... keepLive) {
            output_ = requireNonNull(output);
            args_ = requireNonNull(args);
            url_ = requireNonNull(url);
            this.keep_live_ = new ArrayList<>(Arrays.asList(keepLive));
        }

        private void fail_() {
            final Stream<Metric> metrics = Stream.of(new SimpleMetric(MN_UP, MetricValue.FALSE));
            output_.complete(metrics);
            keep_live_.clear();  // No need to keep objects alive anymore.
        }

        @Override
        public void completed(HttpResponse response) {
            Stream<Metric> metrics;
            try {
                metrics = do_response_(begin_ts, response);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "error processing response for " + url_ + ", args " + args_, ex);
                metrics = Stream.of(new SimpleMetric(MN_UP, MetricValue.FALSE));
            }

            output_.complete(metrics);
            keep_live_.clear();  // No need to keep objects alive anymore.
        }

        @Override
        public void failed(Exception ex) {
            LOG.log(Level.WARNING, "error processing response for " + url_ + ", args " + args_, ex);
            fail_();
        }

        @Override
        public void cancelled() {
            fail_();
        }
    };

    private CompletableFuture<Stream<Metric>> do_request_(GroupName args, String url) {
        /*
         * Client seems to spontaneously stop its reactor.
         * No idea why, so in order to keep the reactor shutdown from disabling
         * the monitor entirely, we allow for resetting it.
         *
         * Since restarted clients drop their reference, keep a reference to the client locally.
         * The client needs to stay alive until the request completes,
         * so we attach it to the response consumer as a keep_live object.
         */
        final GCCloseable<CloseableHttpAsyncClient> client = httpClient();

        final CompletableFuture<Stream<Metric>> result = new CompletableFuture<>();  // Filled in by HttpResponseConsumer instance.
        final HttpGet request = new HttpGet(url);
        request.setConfig(request_config_);
        client.get().execute(request, new HttpResponseConsumer(result, args, url, client));
        return result;
    }

    @Override
    public Collection<CompletableFuture<? extends Collection<? extends MetricGroup>>> getGroups(Executor threadpool, CompletableFuture<TimeoutObject> timeout) throws Exception {
        CompletableFuture<Stream<Metric>> timeoutMetric = timeout
                .thenApply(ignored -> {
                    return Stream.of(new SimpleMetric(MN_UP, MetricValue.FALSE));
                });

        /* Collect all URLs. */
        final List<CompletableFuture<? extends Collection<? extends MetricGroup>>> urls;
        try {
            return patterns.getUrls()
                    .map(x -> {
                        CompletableFuture<Stream<Metric>> reqMetrics
                                = do_request_(x.getKey(), x.getValue());
                        return reqMetrics.applyToEither(
                                timeoutMetric,
                                metrics -> {
                                    final GroupName groupName = groupNameFromArgs(x.getKey());
                                    return singleton(new SimpleMetricGroup(groupName, metrics));
                                });
                    })
                    .collect(Collectors.toList());
        } catch (Exception ex) {
            throw new Exception("unable to load URLs", ex);
        }
    }

    private GroupName groupNameFromArgs(GroupName args) {
        final SimpleGroupPath path = SimpleGroupPath.valueOf(Stream.of(baseGroupName, args.getPath())
                .map(SimpleGroupPath::getPath)
                .flatMap(List::stream)
                .collect(Collectors.toList()));
        return GroupName.valueOf(path, args.getTags());
    }
}
