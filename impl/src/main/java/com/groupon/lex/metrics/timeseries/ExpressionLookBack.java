package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.lib.ForwardIterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public interface ExpressionLookBack {
    public static ChainableExpressionLookBack EMPTY = new ChainableExpressionLookBack() {
        @Override
        public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) { return Stream.empty(); }
        @Override
        public Duration hintDuration() { return Duration.ZERO; }
        @Override
        public ExpressionLookBack andThen(@NonNull ExpressionLookBack next) { return next; }
        @Override
        public ExpressionLookBack andThen(@NonNull Stream<ExpressionLookBack> children) {
            final List<ExpressionLookBack> chain = children.collect(Collectors.toList());
            if (chain.isEmpty()) return this;
            if (chain.size() == 1) return andThen(chain.get(0));
            return new ExpressionLookBack() {
                @Override
                public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
                    return chain.stream().flatMap(elb -> elb.filter(tsc.clone()));
                }
                @Override
                public Duration hintDuration() {
                    return chain.stream().map(ExpressionLookBack::hintDuration).max(Comparator.naturalOrder()).orElse(Duration.ZERO);
                }
            };
        }
    };

    /**
     * Filter the TimeSeriesCollections we want to keep active.
     *
     * @param tsc A list of TimeSeriesCollection currently being kept.
     * @return A stream with the TimeSeriesCollections we want to keep active.
     *     The returned stream may contain duplicate entries.
     */
    public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc);
    /**
     * Give an estimate of how much history has to be maintained, as a time interval.
     * This is mainly an indication of how far in the past a history update would be needed.
     */
    public Duration hintDuration();

    @Value
    public static class ScrapeCount implements ChainableExpressionLookBack {
        private final int count;

        public ScrapeCount(int count) {
            if (count < 0) throw new IllegalArgumentException("cannot look back negative amounts");
            this.count = count;
        }

        private <TSC extends TimeSeriesCollection> Stream<TSC> filter_(@NonNull ForwardIterator<TSC> tsc) {
            final List<TSC> local_result = new ArrayList<>(count);
            for (int n = 0; n < count; ++n) {
                if (!tsc.hasNext()) return local_result.stream();
                local_result.add(tsc.next());
            }
            return local_result.stream();
        }

        @Override
        public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
            return filter_(tsc);
        }

        @Override
        public Duration hintDuration() { return Duration.ZERO; }

        @Override
        public ExpressionLookBack andThen(@NonNull ExpressionLookBack next) {
            return new ExpressionLookBack() {
                @Override
                public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
                    final Stream<TSC> local_result = filter_(tsc);
                    if (!tsc.hasNext()) return local_result;
                    return Stream.concat(local_result, next.filter(tsc));
                }
                @Override
                public Duration hintDuration() {
                    return next.hintDuration();
                }
            };
        }

        @Override
        public ExpressionLookBack andThen(@NonNull Stream<ExpressionLookBack> children) {
            final List<ExpressionLookBack> chain = children.collect(Collectors.toList());
            if (chain.isEmpty()) return this;
            if (chain.size() == 1) return andThen(chain.get(0));
            return new ExpressionLookBack() {
                @Override
                public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
                    final Stream<TSC> local_result = filter_(tsc);
                    if (!tsc.hasNext()) return local_result;
                    return Stream.concat(local_result, chain.stream().flatMap(elb -> elb.filter(tsc.clone())));
                }
                @Override
                public Duration hintDuration() {
                    return chain.stream().map(ExpressionLookBack::hintDuration).max(Comparator.naturalOrder()).orElse(Duration.ZERO);
                }
            };
        }
    }

    @Value
    public static class Interval implements ChainableExpressionLookBack {
        private final Duration duration;

        @AllArgsConstructor
        @Getter
        private static class FilterResult<TSC extends TimeSeriesCollection> {
            private final Stream<TSC> used;
            private final ForwardIterator<TSC> unused;
        }

        public Interval(@NonNull Duration duration) {
            if (duration.isShorterThan(Duration.ZERO)) throw new IllegalArgumentException("negative duration not supported");
            this.duration = duration;
        }

        private <TSC extends TimeSeriesCollection> FilterResult<TSC> filter_(@NonNull ForwardIterator<TSC> tsc) {
            if (!tsc.hasNext()) return new FilterResult(Stream.empty(), tsc);
            final List<TSC> local_result = new ArrayList<>();
            final TSC head = tsc.next();
            local_result.add(head);
            final DateTime oldest_preserve = head.getTimestamp().minus(duration);

            while (tsc.hasNext()) {
                final TSC next = tsc.next();
                local_result.add(next);
                if (!next.getTimestamp().isAfter(oldest_preserve)) break;
            }

            return new FilterResult(local_result.stream(), tsc);
        }

        @Override
        public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
            return filter_(tsc).getUsed();
        }
        @Override
        public Duration hintDuration() { return duration; }

        @Override
        public ExpressionLookBack andThen(@NonNull ExpressionLookBack elb) {
            return new ExpressionLookBack() {
                @Override
                public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
                    final FilterResult<TSC> fr = filter_(tsc);
                    Stream<TSC> result = fr.getUsed();
                    if (fr.getUnused().hasNext())
                        result = Stream.concat(result, elb.filter(fr.getUnused()));
                    return result;
                }
                @Override
                public Duration hintDuration() {
                    return duration.plus(elb.hintDuration());
                }
            };
        }

        @Override
        public ExpressionLookBack andThen(@NonNull Stream<ExpressionLookBack> children) {
            final List<ExpressionLookBack> chain = children.collect(Collectors.toList());
            if (chain.isEmpty()) return this;
            if (chain.size() == 1) return andThen(chain.get(0));
            return new ExpressionLookBack() {
                @Override
                public <TSC extends TimeSeriesCollection> Stream<TSC> filter(@NonNull ForwardIterator<TSC> tsc) {
                    final FilterResult<TSC> fr = filter_(tsc);
                    Stream<TSC> result = fr.getUsed();
                    if (fr.getUnused().hasNext())
                        result = Stream.concat(result, chain.stream().flatMap(elb -> elb.filter(fr.getUnused().clone())));
                    return result;
                }
                @Override
                public Duration hintDuration() {
                    return duration.plus(chain.stream().map(ExpressionLookBack::hintDuration).max(Comparator.naturalOrder()).orElse(Duration.ZERO));
                }
            };
        }
    }

    public static ChainableExpressionLookBack fromInterval(Duration interval) { return new Interval(interval); }
    public static ChainableExpressionLookBack fromScrapeCount(int count) { return new ScrapeCount(count); }
}
