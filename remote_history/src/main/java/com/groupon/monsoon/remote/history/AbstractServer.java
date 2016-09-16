/*
 * Copyright (c) 2016, Ariane van der Steldt
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
package com.groupon.monsoon.remote.history;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.BufferedIterator;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.monsoon.remote.history.xdr.duration_msec;
import com.groupon.monsoon.remote.history.xdr.evaluate_iter_response;
import com.groupon.monsoon.remote.history.xdr.evaluate_response;
import com.groupon.monsoon.remote.history.xdr.list_of_timeseries_collection;
import com.groupon.monsoon.remote.history.xdr.named_evaluation_map;
import com.groupon.monsoon.remote.history.xdr.rh_protoServerStub;
import com.groupon.monsoon.remote.history.xdr.stream_iter_tsc_response;
import com.groupon.monsoon.remote.history.xdr.stream_response;
import com.groupon.monsoon.remote.history.xdr.timestamp_msec;
import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.server.OncRpcCallInformation;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public abstract class AbstractServer extends rh_protoServerStub {
    /**
     * We try to maintain at most MAX_REQUEST_DURATION in an iterator fetch request.
     * If we exceed that time, bail out early and return a short result.
     * We always emit at least 1 result.
     */
    public static final Duration MAX_REQUEST_DURATION = Duration.millis(800);

    private static final Logger LOG = Logger.getLogger(AbstractServer.class.getName());
    /** Default port for the RPC server. */
    public static final int DEFAULT_PORT = Client.DEFAULT_PORT;
    /** Limit TimeSeriesCollection fetch size. */
    private static final int MAX_TSC_FETCH = 50;
    /** Limit evaluation fetch size. */
    private static final int MAX_EVAL_FETCH = 500;
    /** Background prefetch queue size for TimeSeriesCollection iterators. */
    private static final int TSC_QUEUE_SIZE = MAX_TSC_FETCH;
    /** Background prefetch queue size for evaluation iterators. */
    private static final int EVAL_QUEUE_SIZE = MAX_EVAL_FETCH;
    /** Cache iterators, so subsequent requests can refer to existing iterator. */
    private static final Cache<Long, IteratorAndCookie<TimeSeriesCollection>> TSC_ITERS = CacheBuilder.newBuilder()
                .expireAfterAccess(5, TimeUnit.MINUTES)
                .softValues()
                .build();
    /** Allocator for generating unique iterator IDs. */
    private static final AtomicLong TSC_ITERS_ALLOC = new AtomicLong();

    /** Cache iterators, so subsequent requests can refer to existing iterator. */
    private static final Cache<Long, IteratorAndCookie<Collection<CollectHistory.NamedEvaluation>>> EVAL_ITERS = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .softValues()
                .build();
    /** Allocator for generating unique iterator IDs. */
    private static final AtomicLong EVAL_ITERS_ALLOC = new AtomicLong();

    public AbstractServer() throws OncRpcException, IOException {
        super();
    }

    public AbstractServer(int port) throws OncRpcException, IOException {
        super(port);
    }

    public AbstractServer(InetAddress bindAddr, int port) throws OncRpcException, IOException {
        super(bindAddr, port);
    }

    /** Create a new TimeSeriesCollection iterator from the given stream. */
    private static stream_response newTscStream(Stream<TimeSeriesCollection> tsc, int fetch) {
        final Iterator<TimeSeriesCollection> iter = BufferedIterator.iterator(ForkJoinPool.commonPool(), tsc.iterator(), TSC_QUEUE_SIZE);
        final long idx = TSC_ITERS_ALLOC.getAndIncrement();
        final IteratorAndCookie<TimeSeriesCollection> iterAndCookie = new IteratorAndCookie<>(iter);
        TSC_ITERS.put(idx, iterAndCookie);

        final List<TimeSeriesCollection> result = fetchFromIter(iter, fetch, MAX_TSC_FETCH);
        EncDec.NewIterResponse<TimeSeriesCollection> responseObj =
                new EncDec.NewIterResponse<>(idx, result, !iter.hasNext(), iterAndCookie.getCookie());
        LOG.log(Level.FINE, "responseObj = {0}", responseObj);
        return EncDec.encodeStreamResponse(responseObj);
    }

    /** Create a new evaluation iterator from the given stream. */
    private static evaluate_response newEvalStream(Stream<Collection<CollectHistory.NamedEvaluation>> tsc, int fetch) {
        final Iterator<Collection<CollectHistory.NamedEvaluation>> iter = BufferedIterator.iterator(ForkJoinPool.commonPool(), tsc.iterator(), EVAL_QUEUE_SIZE);
        final long idx = EVAL_ITERS_ALLOC.getAndIncrement();
        final IteratorAndCookie<Collection<CollectHistory.NamedEvaluation>> iterAndCookie = new IteratorAndCookie<>(iter);
        EVAL_ITERS.put(idx, iterAndCookie);

        final List<Collection<CollectHistory.NamedEvaluation>> result = fetchFromIter(iter, fetch, MAX_EVAL_FETCH);
        EncDec.NewIterResponse<Collection<CollectHistory.NamedEvaluation>> responseObj =
                new EncDec.NewIterResponse<>(idx, result, !iter.hasNext(), iterAndCookie.getCookie());
        LOG.log(Level.FINE, "responseObj = {0}", responseObj);
        return EncDec.encodeEvaluateResponse(responseObj);
    }

    /**
     * Fetch up to a given amount of items from the iterator.
     * @param <T> The type of elements in the iterator.
     * @param iter The iterator supplying items.
     * @param fetch The requested number of items to fetch (user supplied parameter).
     * @param max_fetch The hard limit on how many items to fetch.
     * @return A list with items fetched from the iterator.
     */
    private static <T> List<T> fetchFromIter(Iterator<T> iter, int fetch, int max_fetch) {
        final long t0 = System.currentTimeMillis();
        assert(max_fetch >= 1);
        if (fetch < 0 || fetch > max_fetch) fetch = max_fetch;

        final List<T> result = new ArrayList<>(fetch);
        for (int i = 0; i < fetch && iter.hasNext(); ++i) {
            result.add(iter.next());

            // Decide if we should cut the fetch short.
            // Note that we always emit at least 1 element.
            final long tCur = System.currentTimeMillis();
            if (tCur - t0 >= MAX_REQUEST_DURATION.getMillis())
                break;
        }
        return result;
    }

    public abstract boolean addTSData(List<TimeSeriesCollection> c);
    public abstract long getFileSize();
    public abstract DateTime getEnd();
    public abstract Stream<TimeSeriesCollection> streamReverse();
    public Stream<TimeSeriesCollection> streamReverse(DateTime from) {
        return streamReverse()
                .filter((TimeSeriesCollection tsc) -> !tsc.getTimestamp().isAfter(from));
    }
    public abstract Stream<TimeSeriesCollection> stream();
    public abstract Stream<TimeSeriesCollection> stream(DateTime begin);
    public abstract Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end);
    public abstract Stream<TimeSeriesCollection> stream(Duration stepSize);
    public abstract Stream<TimeSeriesCollection> stream(DateTime begin, Duration stepSize);
    public abstract Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end, Duration stepSize);
    public abstract Stream<Collection<CollectHistory.NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> query, Duration stepSize);
    public abstract Stream<Collection<CollectHistory.NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> query, DateTime begin, Duration stepSize);
    public abstract Stream<Collection<CollectHistory.NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> query, DateTime begin, DateTime end, Duration stepSize);

    @Override
    public final boolean addTSData_1(list_of_timeseries_collection c) {
        return addTSData(EncDec.decodeTSCCollection(c));
    }

    @Override
    public final long getFileSize_1() {
        return getFileSize();
    }

    @Override
    public final timestamp_msec getEnd_1() {
        return EncDec.encodeTimestamp(getEnd());
    }

    @Override
    public final stream_iter_tsc_response streamIterTscNext_1(long id, long cookie, int fetch) {
        LOG.log(Level.FINE, "TSC iter next({0}, {1})", new Object[]{id, fetch});
        IteratorAndCookie<TimeSeriesCollection> iter = TSC_ITERS.getIfPresent(id);
        if (iter == null || !iter.update(cookie))
            return EncDec.encodeStreamIterTscResponse(new EncDec.IterErrorResponse(IteratorErrorCode.UNKNOWN_ITERATOR));

        final List<TimeSeriesCollection> result = fetchFromIter(iter.getIterator(), fetch, MAX_TSC_FETCH);
        return EncDec.encodeStreamIterTscResponse(new EncDec.IterSuccessResponseImpl<>(result, !iter.getIterator().hasNext(), iter.getCookie()));
    }

    @Override
    public void closeIterTsc_1(long id, long cookie) {
        final IteratorAndCookie<TimeSeriesCollection> iter = TSC_ITERS.getIfPresent(id);
        if (iter != null && iter.update(cookie))
            TSC_ITERS.invalidate(id);
    }

    @Override
    public final stream_response streamReverse_1(int fetch) {
        return newTscStream(streamReverse(), fetch);
    }

    @Override
    public final stream_response streamReverseFrom_1(timestamp_msec begin, int fetch) {
        return newTscStream(streamReverse(EncDec.decodeTimestamp(begin)), fetch);
    }

    @Override
    public final stream_response stream_1(int fetch) {
        return newTscStream(stream(), fetch);
    }

    @Override
    public final stream_response streamFrom_1(timestamp_msec begin, int fetch) {
        return newTscStream(stream(EncDec.decodeTimestamp(begin)), fetch);
    }

    @Override
    public final stream_response streamFromTo_1(timestamp_msec begin, timestamp_msec end, int fetch) {
        return newTscStream(stream(EncDec.decodeTimestamp(begin), EncDec.decodeTimestamp(end)), fetch);
    }

    @Override
    public final stream_response streamStepped_1(duration_msec stepSize, int fetch) {
        return newTscStream(stream(EncDec.decodeDuration(stepSize)), fetch);
    }

    @Override
    public final stream_response streamSteppedFrom_1(timestamp_msec begin, duration_msec stepSize, int fetch) {
        return newTscStream(stream(EncDec.decodeTimestamp(begin), EncDec.decodeDuration(stepSize)), fetch);
    }

    @Override
    public final stream_response streamSteppedFromTo_1(timestamp_msec begin, timestamp_msec end, duration_msec stepSize, int fetch) {
        return newTscStream(stream(EncDec.decodeTimestamp(begin), EncDec.decodeTimestamp(end), EncDec.decodeDuration(stepSize)), fetch);
    }

    @Override
    public final evaluate_iter_response evaluateIterNext_1(long id, long cookie, int fetch) {
        LOG.log(Level.FINE, "eval iter next({0}, {1})", new Object[]{id, fetch});
        IteratorAndCookie<Collection<CollectHistory.NamedEvaluation>> iter = EVAL_ITERS.getIfPresent(id);
        if (iter == null || !iter.update(cookie))
            return EncDec.encodeEvaluateIterResponse(new EncDec.IterErrorResponse(IteratorErrorCode.UNKNOWN_ITERATOR));

        final List<Collection<CollectHistory.NamedEvaluation>> result = fetchFromIter(iter.getIterator(), fetch, MAX_EVAL_FETCH);
        return EncDec.encodeEvaluateIterResponse(new EncDec.IterSuccessResponseImpl<>(result, !iter.getIterator().hasNext(), iter.getCookie()));
    }

    @Override
    public final void closeEvalIter_1(long id, long cookie) {
        final IteratorAndCookie<Collection<CollectHistory.NamedEvaluation>> iter = EVAL_ITERS.getIfPresent(id);
        if (iter != null && iter.update(cookie))
            EVAL_ITERS.invalidate(id);
    }

    @Override
    public final evaluate_response evaluate_1(named_evaluation_map query, duration_msec stepSize, int fetch) {
        try {
            return newEvalStream(evaluate(EncDec.decodeEvaluationMap(query), EncDec.decodeDuration(stepSize)), fetch);
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "unable to evaluate", ex);
            throw ex;
        }
    }

    @Override
    public final evaluate_response evaluateFrom_1(named_evaluation_map query, timestamp_msec begin, duration_msec stepSize, int fetch) {
        try {
            return newEvalStream(evaluate(EncDec.decodeEvaluationMap(query), EncDec.decodeTimestamp(begin), EncDec.decodeDuration(stepSize)), fetch);
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "unable to evaluate", ex);
            throw ex;
        }
    }

    @Override
    public final evaluate_response evaluateFromTo_1(named_evaluation_map query, timestamp_msec begin, timestamp_msec end, duration_msec stepSize, int fetch) {
        try {
            return newEvalStream(evaluate(EncDec.decodeEvaluationMap(query), EncDec.decodeTimestamp(begin), EncDec.decodeTimestamp(end), EncDec.decodeDuration(stepSize)), fetch);
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "unable to evaluate", ex);
            throw ex;
        }
    }

    @RequiredArgsConstructor
    @Getter
    private static class IteratorAndCookie<T> {
        private static final SecureRandom RANDOM = new SecureRandom();
        private final Iterator<T> iterator;
        private long cookie = RANDOM.nextLong();

        /**
         * Change the cookie.
         * @return True indicating the expected value matched and the cookie was changed.
         *   If the operation fails, false is returned.
         */
        synchronized public boolean update(long expected) {
            if (cookie != expected) return false;

            long newCookie;
            do {
                newCookie = RANDOM.nextLong();
            } while (cookie == newCookie);  // Always change the cookie.
            cookie = newCookie;

            return true;
        }
    }

    @Override
    public void dispatchOncRpcCall(OncRpcCallInformation call, int program, int version, int procedure) throws OncRpcException, IOException {
        try {
            super.dispatchOncRpcCall(call, program, version, procedure);
        } catch (RuntimeException | OncRpcException | IOException ex) {
            LOG.log(Level.WARNING, "error for RPC call(" + call + ", " + program + ", " + version + ", " + procedure + ")", ex);
            throw ex;
        }
    }

}
