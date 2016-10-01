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

import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import static com.groupon.monsoon.remote.history.EncDec.decodeTimestamp;
import static com.groupon.monsoon.remote.history.EncDec.encodeTSCCollection;
import com.groupon.monsoon.remote.history.xdr.list_of_timeseries_collection;
import com.groupon.monsoon.remote.history.xdr.named_evaluation_map;
import com.groupon.monsoon.remote.history.xdr.rh_protoClient;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.OncRpcProtocols;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class Client implements CollectHistory {
    /** Default port for the RPC server. */
    public static final int DEFAULT_PORT = 9996;
    private static final int TSC_INITIAL_BATCH_SIZE = 10;
    private static final int TSC_BATCH_SIZE = 50;
    private static final int EVAL_INITIAL_BATCH_SIZE = 50;
    private static final int EVAL_BATCH_SIZE = 250;
    private static final Logger LOG = Logger.getLogger(Client.class.getName());
    private final InetAddress host;
    private final int port;
    private final OptionalInt protocolOverride;

    public Client(InetAddress host, int port, OptionalInt protocolOverride) throws OncRpcException, IOException {
        if (port == 0) port = DEFAULT_PORT;
        this.host = host;
        this.port = port;
        this.protocolOverride = protocolOverride;

        final rh_protoClient client = getRpcClient(OncRpcProtocols.ONCRPC_UDP);
        try {
            BlockingWrapper.execute(() -> client.nullproc());  // Test the connection.
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } finally {
            client.close();
        }
    }

    public Client(InetAddress host, int port) throws OncRpcException, IOException {
        this(host, port, OptionalInt.empty());
    }

    public Client(InetAddress host, int port, int protocol) throws OncRpcException, IOException {
        this(host, port, OptionalInt.of(protocol));
    }

    private rh_protoClient getRpcClient(int protocol) throws IOException, OncRpcException {
        final rh_protoClient client = new rh_protoClient(this.host, this.port, protocolOverride.orElse(protocol));
        client.getClient().setCharacterEncoding("UTF-8");
        return client;
    }

    /** Add a TimeSeriesCollection to the history. */
    @Override
    public boolean add(TimeSeriesCollection tsv) {
        return addAll(singletonList(tsv));
    }

    /** Add multiple TimeSeriesCollections to the history. */
    @Override
    public boolean addAll(Collection<? extends TimeSeriesCollection> c) {
        try {
            final rh_protoClient client = getRpcClient(OncRpcProtocols.ONCRPC_TCP);
            try {
                final list_of_timeseries_collection enc_c = encodeTSCCollection(c);
                return BlockingWrapper.execute(() -> client.addTSData_1(enc_c));
            } finally {
                client.close();
            }
        } catch (OncRpcException | IOException | InterruptedException | RuntimeException ex) {
            LOG.log(Level.SEVERE, "addAll RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Check disk space usage (bytes).
     * @return The disk space used for storing metrics.
     */
    @Override
    public long getFileSize() {
        try {
            final rh_protoClient client = getRpcClient(OncRpcProtocols.ONCRPC_UDP);
            try {
                return BlockingWrapper.execute(() -> client.getFileSize_1());
            } finally {
                client.close();
            }
        } catch (OncRpcException | IOException | InterruptedException | RuntimeException ex) {
            LOG.log(Level.SEVERE, "getFileSize RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Return the highest timestamp in the stored metrics.
     * @return The highest timestamp in the stored metrics.
     */
    @Override
    public DateTime getEnd() {
        try {
            final rh_protoClient client = getRpcClient(OncRpcProtocols.ONCRPC_UDP);
            try {
                return decodeTimestamp(BlockingWrapper.execute(() -> client.getEnd_1()));
            } finally {
                client.close();
            }
        } catch (OncRpcException | IOException | InterruptedException | RuntimeException ex) {
            LOG.log(Level.SEVERE, "getEnd RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the history in reverse chronological order.
     * @return A TimeSeriesCollection stream, in reverse chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> streamReversed() {
        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        if (ts == null)
                            return EncDec.decodeStreamResponse(rpcClient.streamReverse_1(TSC_INITIAL_BATCH_SIZE));
                        else
                            return EncDec.decodeStreamResponse(rpcClient.streamReverseFrom_1(EncDec.encodeTimestamp(ts), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().minus(1));
                    },
                    null);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the entire history.
     * @return A TimeSeriesCollection stream, in chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> stream() {
        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        if (ts == null)
                            return EncDec.decodeStreamResponse(rpcClient.stream_1(TSC_INITIAL_BATCH_SIZE));
                        else
                            return EncDec.decodeStreamResponse(rpcClient.streamFrom_1(EncDec.encodeTimestamp(ts), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().plus(1));
                    },
                    null);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the entire history.
     * @param stepSizeArg The minimum time difference between metrics.
     * @return A TimeSeriesCollection stream, in chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> stream(Duration stepSizeArg) {
        final Duration stepSize;
        if (!stepSizeArg.isLongerThan(Duration.ZERO))
            stepSize = new Duration(1);
        else
            stepSize = stepSizeArg;

        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        if (ts == null)
                            return EncDec.decodeStreamResponse(rpcClient.streamStepped_1(EncDec.encodeDuration(stepSize), TSC_INITIAL_BATCH_SIZE));
                        else
                            return EncDec.decodeStreamResponse(rpcClient.streamSteppedFrom_1(EncDec.encodeTimestamp(ts), EncDec.encodeDuration(stepSize), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().plus(stepSize));
                    },
                    null);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the history, starting at the given timestamp.
     * @param begin The timestamp from which to start iterating the history.
     * @return A TimeSeriesCollection stream, in chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        return EncDec.decodeStreamResponse(rpcClient.streamFrom_1(EncDec.encodeTimestamp(ts), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().plus(1));
                    },
                    begin);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the history, starting at the given timestamp.
     * @param begin The timestamp from which to start iterating the history.
     * @param stepSizeArg The minimum time difference between metrics.
     * @return A TimeSeriesCollection stream, in chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, Duration stepSizeArg) {
        final Duration stepSize;
        if (!stepSizeArg.isLongerThan(Duration.ZERO))
            stepSize = new Duration(1);
        else
            stepSize = stepSizeArg;

        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        return EncDec.decodeStreamResponse(rpcClient.streamSteppedFrom_1(EncDec.encodeTimestamp(ts), EncDec.encodeDuration(stepSize), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().plus(stepSize));
                    },
                    begin);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the history, between the given timestamps.
     * @param begin The timestamp from which to start iterating the history.
     * @param end The timestamp (inclusive) at which to stop iterating the history.
     * @return A TimeSeriesCollection stream, in chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        return EncDec.decodeStreamResponse(rpcClient.streamFromTo_1(EncDec.encodeTimestamp(ts), EncDec.encodeTimestamp(end), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().plus(1));
                    },
                    begin);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Iterate the history, between the given timestamps.
     * @param begin The timestamp from which to start iterating the history.
     * @param end The timestamp (inclusive) at which to stop iterating the history.
     * @param stepSizeArg The minimum time difference between metrics.
     * @return A TimeSeriesCollection stream, in chronological order.
     */
    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end, Duration stepSizeArg) {
        final Duration stepSize;
        if (!stepSizeArg.isLongerThan(Duration.ZERO))
            stepSize = new Duration(1);
        else
            stepSize = stepSizeArg;

        try {
            final RpcIterator<TimeSeriesCollection> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        return EncDec.decodeStreamResponse(rpcClient.streamSteppedFromTo_1(EncDec.encodeTimestamp(ts), EncDec.encodeTimestamp(end), EncDec.encodeDuration(stepSize), TSC_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeStreamIterTscResponse(rpcClient.streamIterTscNext_1(id, cookie, TSC_BATCH_SIZE));
                    },
                    (data) -> {
                        return Optional.of(data.get(data.size() - 1).getTimestamp().plus(stepSize));
                    },
                    begin);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Evaluate expressions over the history.
     * @param expression The query describing one or more expressions to evaluate.
     * @param stepSizeArg The minimum time difference between metrics.
     * @return A stream of evaluations, in chronological order.
     */
    @Override
    public Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, Duration stepSizeArg) {
        final Duration stepSize;
        if (!stepSizeArg.isLongerThan(Duration.ZERO))
            stepSize = new Duration(1);
        else
            stepSize = stepSizeArg;

        try {
            final named_evaluation_map query = EncDec.encodeEvaluationMap(expression);
            final RpcIterator<Collection<NamedEvaluation>> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        if (ts == null)
                            return EncDec.decodeEvaluateResponse(rpcClient.evaluate_1(query, EncDec.encodeDuration(stepSize), EVAL_INITIAL_BATCH_SIZE));
                        else
                            return EncDec.decodeEvaluateResponse(rpcClient.evaluateFrom_1(query, EncDec.encodeTimestamp(ts), EncDec.encodeDuration(stepSize), EVAL_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeEvaluateIterResponse(rpcClient.evaluateIterNext_1(id, cookie, EVAL_BATCH_SIZE));
                    },
                    (data) -> {
                        return data.stream()
                                .flatMap(Collection::stream)
                                .map(NamedEvaluation::getDatetime)
                                .max(Comparator.naturalOrder())
                                .map(ts -> ts.plus(stepSize));
                    },
                    null);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Evaluate expressions over the history, starting at the given timestamp.
     * @param expression The query describing one or more expressions to evaluate.
     * @param begin The starting point for iteration.
     * @param stepSizeArg The minimum time difference between metrics.
     * @return A stream of evaluations, in chronological order.
     */
    @Override
    public Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, DateTime begin, Duration stepSizeArg) {
        final Duration stepSize;
        if (!stepSizeArg.isLongerThan(Duration.ZERO))
            stepSize = new Duration(1);
        else
            stepSize = stepSizeArg;

        try {
            final named_evaluation_map query = EncDec.encodeEvaluationMap(expression);
            final RpcIterator<Collection<NamedEvaluation>> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        return EncDec.decodeEvaluateResponse(rpcClient.evaluateFrom_1(query, EncDec.encodeTimestamp(ts), EncDec.encodeDuration(stepSize), EVAL_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeEvaluateIterResponse(rpcClient.evaluateIterNext_1(id, cookie, EVAL_BATCH_SIZE));
                    },
                    (data) -> {
                        return data.stream()
                                .flatMap(Collection::stream)
                                .map(NamedEvaluation::getDatetime)
                                .max(Comparator.naturalOrder())
                                .map(ts -> ts.plus(stepSize));
                    },
                    begin);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Evaluate expressions over the history, between the given timestamps.
     * @param expression The query describing one or more expressions to evaluate.
     * @param begin The starting point for iteration.
     * @param end The end point (inclusive) for iteration.
     * @param stepSizeArg The minimum time difference between metrics.
     * @return A stream of evaluations, in chronological order.
     */
    @Override
    public Stream<Collection<NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> expression, DateTime begin, DateTime end, Duration stepSizeArg) {
        final Duration stepSize;
        if (!stepSizeArg.isLongerThan(Duration.ZERO))
            stepSize = new Duration(1);
        else
            stepSize = stepSizeArg;

        try {
            final named_evaluation_map query = EncDec.encodeEvaluationMap(expression);
            final RpcIterator<Collection<NamedEvaluation>> iter = new RpcIterator<>(
                    getRpcClient(OncRpcProtocols.ONCRPC_TCP),
                    (rh_protoClient rpcClient, DateTime ts) -> {
                        return EncDec.decodeEvaluateResponse(rpcClient.evaluateFromTo_1(query, EncDec.encodeTimestamp(ts), EncDec.encodeTimestamp(end), EncDec.encodeDuration(stepSize), EVAL_INITIAL_BATCH_SIZE));
                    },
                    (rh_protoClient rpcClient, long id, long cookie) -> {
                        return EncDec.decodeEvaluateIterResponse(rpcClient.evaluateIterNext_1(id, cookie, EVAL_BATCH_SIZE));
                    },
                    (data) -> {
                        return data.stream()
                                .flatMap(Collection::stream)
                                .map(NamedEvaluation::getDatetime)
                                .max(Comparator.naturalOrder())
                                .map(ts -> ts.plus(stepSize));
                    },
                    begin);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, NONNULL | IMMUTABLE | ORDERED), false);
        } catch (OncRpcException | IOException ex) {
            LOG.log(Level.SEVERE, "stream RPC call failed", ex);
            throw new RuntimeException("RPC call failed", ex);
        }
    }

    /**
     * Internal iterator type that iterates remote iterator instances.
     *
     * The iterator is able to resume if the remote iterator disappears.
     */
    private static class RpcIterator<T> implements Iterator<T> {
        private long id;
        private rh_protoClient rpcClient;
        private boolean fin = false;
        private List<? extends T> nextValues = EMPTY_LIST;
        private DateTime restartTS;
        private long cookie;
        private final Function<List<? extends T>, Optional<DateTime>> computeRestartTs;
        private final RpcCall<DateTime, EncDec.NewIterResponse<T>> restartCall;
        private final RpcContinue<T> continueCall;

        public RpcIterator(@NonNull rh_protoClient rpcClient,
                @NonNull RpcCall<DateTime, EncDec.NewIterResponse<T>> restartCall,
                @NonNull RpcContinue<T> continueCall,
                @NonNull Function<List<? extends T>, Optional<DateTime>> computeRestartTs,
                DateTime initialTime) throws IOException, OncRpcException {
            this.rpcClient = rpcClient;
            this.restartTS = initialTime;
            this.computeRestartTs = computeRestartTs;
            this.restartCall = restartCall;
            this.continueCall = continueCall;

            try {
                final EncDec.NewIterResponse<T> sr;
                try {
                    sr = BlockingWrapper.execute(() -> this.restartCall.call(this.rpcClient, this.restartTS));
                } catch (InterruptedException ex) {
                    throw new RuntimeException("interrupted during start of iterator", ex);
                }
                id = sr.getIterIdx();
                applyValues(sr);
            } catch (RuntimeException | IOException | OncRpcException ex) {
                LOG.log(Level.WARNING, "iterator creation failed", ex);
                try {
                    this.rpcClient.close();
                } catch (Exception ex1) {
                    ex.addSuppressed(ex1);
                }
                throw ex;
            }
        }

        @Override
        public boolean hasNext() {
            ensureNextValues();
            return !nextValues.isEmpty();
        }

        @Override
        public T next() {
            ensureNextValues();
            if (nextValues.isEmpty()) {
                assert(fin);
                throw new NoSuchElementException("no more values");
            }
            return nextValues.remove(0);
        }

        private void ensureNextValues() {
            if (nextValues.isEmpty()) loadNextValues();
        }

        private void loadNextValues() {
            if (fin) return;
            assert(nextValues.isEmpty());

            do {
                final Any2<EncDec.IterSuccessResponse<T>, EncDec.IterErrorResponse> response;
                try {
                    response = BlockingWrapper.execute(() -> continueCall.call(rpcClient, id, cookie));
                } catch (IOException | OncRpcException | InterruptedException ex) {
                    LOG.log(Level.WARNING, "error fetching next set from iterator", ex);
                    restart();
                    continue;
                }
                final Optional<EncDec.IterSuccessResponse<T>> success = response.getLeft();
                if (success.isPresent()) {
                    applyValues(success.get());
                    continue;
                }

                final Optional<EncDec.IterErrorResponse> lost = response.getRight();
                assert(lost.isPresent());
                switch (lost.get().getError()) {
                    case UNKNOWN_ITERATOR:
                        restart();
                        break;
                }
            } while (nextValues.isEmpty() && !fin);
        }

        private void applyValues(EncDec.IterSuccessResponse<T> sr) {
            assert(this.nextValues.isEmpty());
            this.nextValues = sr.getData();
            this.fin = sr.isLast();
            this.cookie = sr.getCookie();
            if (!this.nextValues.isEmpty())
                computeRestartTs.apply(this.nextValues).ifPresent(ts -> this.restartTS = ts);
            if (fin) {
                try {
                    BlockingWrapper.execute(() -> rpcClient.closeIterTsc_1(id, this.cookie));
                } catch (IOException | OncRpcException | InterruptedException | RuntimeException ex) {
                    /* Ignore, server will drop iterator at some point. */
                }
                try {
                    rpcClient.close();
                } catch (OncRpcException ex) {
                    /* Ignore, let GC handle anything that needs closing using finalization. */
                }
                rpcClient = null;
            }
        }

        private void restart() {
            try {
                final EncDec.NewIterResponse<T> sr = BlockingWrapper.execute(() -> restartCall.call(rpcClient, restartTS));
                id = sr.getIterIdx();
                applyValues(sr);
            } catch (RuntimeException ex) {
                LOG.log(Level.WARNING, "unable to restart iteration", ex);
                throw ex;
            } catch (IOException | OncRpcException | InterruptedException ex) {
                LOG.log(Level.WARNING, "unable to restart iteration", ex);
                throw new RuntimeException("unable to restart iteration", ex);
            }
        }
    }

    private static interface RpcCall<Arg, T> {
        public T call(rh_protoClient rpcClient, Arg arg) throws IOException, OncRpcException;
    }

    private static interface RpcContinue<T> {
        public Any2<EncDec.IterSuccessResponse<T>, EncDec.IterErrorResponse> call(rh_protoClient rpcClient, long id, long cookie) throws IOException, OncRpcException;
    }

    /** Wrapper around RPC calls, to play nice with ForkJoinPool. */
    @RequiredArgsConstructor
    private static class BlockingWrapper<T> implements ForkJoinPool.ManagedBlocker {
        private T result;
        private IOException ioException;
        private OncRpcException oncRpcException;
        private RuntimeException runtimeException;
        private final RpcCall<? extends T> call;

        public static <T> T execute(RpcCall<? extends T> call) throws InterruptedException, IOException, OncRpcException, RuntimeException {
            final BlockingWrapper<T> blocker = new BlockingWrapper<>(call);
            ForkJoinPool.managedBlock(blocker);
            return blocker.get();
        }

        public static void execute(RpcRunnable call) throws InterruptedException, IOException, OncRpcException, RuntimeException {
            final BlockingWrapper<Void> blocker = new BlockingWrapper<>(() -> { call.call(); return null; });
            ForkJoinPool.managedBlock(blocker);
            blocker.get();  // Propagate exceptions.
        }

        public T get() throws IOException, OncRpcException, RuntimeException {
            if (ioException != null) throw ioException;
            if (oncRpcException != null) throw oncRpcException;
            if (runtimeException != null) throw runtimeException;
            return result;
        }

        @Override
        public boolean block() throws InterruptedException {
            if (isReleasable()) return true;
            try {
                result = call.call();
            } catch (InterruptedException ex) {
                throw ex;
            } catch (OncRpcException ex) {
                oncRpcException = ex;
            } catch (IOException ex) {
                ioException = ex;
            } catch (RuntimeException ex) {
                runtimeException = ex;
            }
            return true;
        }

        @Override
        public boolean isReleasable() {
            return result != null || ioException != null || oncRpcException != null || runtimeException != null;
        }

        public static interface RpcCall<T> {
            public T call() throws IOException, OncRpcException, RuntimeException, InterruptedException;
        }

        public static interface RpcRunnable {
            public void call() throws IOException, OncRpcException, RuntimeException, InterruptedException;
        }
    }
}
