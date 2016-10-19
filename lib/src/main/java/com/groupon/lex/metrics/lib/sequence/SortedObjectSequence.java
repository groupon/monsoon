package com.groupon.lex.metrics.lib.sequence;

import com.groupon.lex.metrics.lib.Any2;
import static com.groupon.lex.metrics.lib.sequence.ObjectSequence.reverseComparator;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SortedObjectSequence<T> implements ObjectSequence<T> {
    private final Partition<T> partition;
    private final Comparator<? super T> comparator;
    @Getter
    private final boolean nonnull, distinct;

    public SortedObjectSequence(@NonNull ObjectSequence<T> seq, @NonNull Comparator<? super T> comparator) {
        if (seq.size() <= 1)
            partition = new LeafPartition<>(MappedObjectSequence.identity(seq), comparator);
        else
            partition = new RangePartition<>(MappedObjectSequence.identity(seq), comparator);
        this.comparator = comparator;
        this.nonnull = seq.isNonnull();
        this.distinct = seq.isDistinct();
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public T get(int index) {
        return partition.get(index);
    }

    @Override
    public <C extends Comparable<? super C>> Comparator<C> getComparator() {
        return (Comparator<C>) comparator;
    }

    @Override
    public Iterator<T> iterator() {
        return stream().iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return partition.spliterator(spliteratorCharacteristics());
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(this::spliterator, spliteratorCharacteristics(), false);
    }

    @Override
    public Stream<T> parallelStream() {
        return StreamSupport.stream(this::spliterator, spliteratorCharacteristics(), true);
    }

    @Override
    public int size() {
        return partition.size();
    }

    @Override
    public boolean isEmpty() {
        return partition.isEmpty();
    }

    @Override
    public SortedObjectSequence<T> reverse() {
        return new SortedObjectSequence<>(partition.reverse(), reverseComparator(comparator), nonnull, distinct);
    }

    private static interface Partition<T> {
        public int size();

        public boolean isEmpty();

        public T get(int index);

        public Spliterator<T> spliterator(int characteristics);

        public Partition<T> reverse();
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @ToString
    private static class RangePartition<T> implements Partition<T> {
        private static final SecureRandom RANDOM = new SecureRandom();
        private Any2<MappedObjectSequence<T>, PartitionPair<T>> data;
        private final Comparator<? super T> comparator;

        public RangePartition(MappedObjectSequence<T> data, Comparator<? super T> comparator) {
            assert data != null;
            assert data.size() > 1;
            assert comparator != null;
            this.data = Any2.left(data);
            this.comparator = comparator;
        }

        @Override
        public synchronized int size() {
            return data.mapCombine(MappedObjectSequence::size, pair -> pair.getFirst().size() + pair.getSecond().size());
        }

        @Override
        public synchronized boolean isEmpty() {
            return data.mapCombine(MappedObjectSequence::isEmpty, pair -> pair.getFirst().isEmpty() && pair.getSecond().isEmpty());
        }

        @Override
        public T get(int index) {
            PartitionPair<T> pair = makePartition();
            if (index < pair.getFirst().size())
                return pair.getFirst().get(index);
            else
                return pair.getSecond().get(index - pair.getFirst().size());
        }

        @Override
        public Spliterator<T> spliterator(int characteristics) {
            return new SpliteratorImpl(makePartition(), characteristics, comparator);
        }

        @Override
        public RangePartition<T> reverse() {
            return new RangePartition<>(data.map(Function.identity(), PartitionPair::reverse), reverseComparator(comparator));
        }

        private synchronized PartitionPair<T> makePartition() {
            if (data.getRight().isPresent())
                return data.getRight().get();

            final MappedObjectSequence<T> seq = data.getLeft().orElseThrow(IllegalStateException::new);
            int[] partitionMap = new ForwardSequence(0, seq.size()).toArray();
            swap(partitionMap, RANDOM.nextInt(partitionMap.length), 0);  // Choose a pivot and move it to the front.
            int partitionIdx = 0;  // Initial pivot position.

            for (int i = 1; i < partitionMap.length; ++i) {
                if (comparator.compare(seq.get(partitionMap[i]), seq.get(partitionMap[partitionIdx])) < 0) {
                    swap3(partitionMap, partitionIdx, i, partitionIdx + 1);
                    ++partitionIdx;
                }
            }

            final PartitionPair<T> result;
            if (partitionIdx == 0) {
                data = Any2.right(result = new PartitionPair<>(
                        makePartition(seq, Arrays.copyOfRange(partitionMap, 0, partitionIdx + 1), comparator),
                        makePartition(seq, Arrays.copyOfRange(partitionMap, partitionIdx + 1, partitionMap.length), comparator)));
            } else {
                data = Any2.right(result = new PartitionPair<>(
                        makePartition(seq, Arrays.copyOfRange(partitionMap, 0, partitionIdx), comparator),
                        makePartition(seq, Arrays.copyOfRange(partitionMap, partitionIdx, partitionMap.length), comparator)));
            }
            return result;
        }

        private static <T> Partition<T> makePartition(MappedObjectSequence<T> seq, int[] partitionMap, Comparator<? super T> comparator) {
            MappedObjectSequence<T> newSeq = seq.remap(partitionMap);
            if (newSeq.size() <= 1)
                return new LeafPartition<>(newSeq, comparator);
            return new RangePartition<>(newSeq, comparator);
        }

        private static void swap(int[] ints, int index1, int index2) {
            int tmp = ints[index1];
            ints[index1] = ints[index2];
            ints[index2] = tmp;
        }

        private static void swap3(int[] ints, int x, int y, int z) {
            int tmp = ints[x];
            ints[x] = ints[y];
            ints[y] = ints[z];
            ints[z] = tmp;
        }

        @AllArgsConstructor
        @Getter
        @ToString
        private static class PartitionPair<T> {
            @NonNull
            private final Partition<T> first, second;

            public PartitionPair<T> reverse() {
                return new PartitionPair<>(second.reverse(), first.reverse());
            }
        }

        @RequiredArgsConstructor
        private static class SpliteratorImpl<T> implements Spliterator<T> {
            private final PartitionPair<T> partition;
            private int index = 0;
            private Spliterator<T> active = null;
            private final int characteristics;
            private final Comparator<? super T> comparator;

            private void activate() {
                if (active == null) {
                    switch (index) {
                        case 0:
                            active = partition.getFirst().spliterator(characteristics);
                            break;
                        case 1:
                            active = partition.getSecond().spliterator(characteristics);
                            break;
                    }
                }
            }

            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                for (;;) {
                    activate();
                    if (active == null)
                        return false;

                    if (active.tryAdvance(action))
                        return true;
                    active = null;  // Exhausted.
                    ++index;  // Move to the next.
                }
            }

            @Override
            public void forEachRemaining(Consumer<? super T> action) {
                for (;;) {
                    activate();
                    if (active == null)
                        return;

                    active.forEachRemaining(action);
                    active = null;  // Exhausted.
                    ++index;  // Move to the next.
                }
            }

            @Override
            public Spliterator<T> trySplit() {
                activate();
                if (active == null)
                    return null;

                if (index == 0) {
                    Spliterator<T> result = active;
                    active = null;
                    ++index;
                    return result;
                } else {
                    return active.trySplit();
                }
            }

            @Override
            public long estimateSize() {
                activate();
                if (active == null)
                    return 0;

                long sum = active.estimateSize();
                if (index == 0)
                    sum += partition.getSecond().size();
                return sum;
            }

            @Override
            public int characteristics() {
                return characteristics;
            }

            @Override
            public Comparator<? super T> getComparator() {
                return comparator;
            }
        }
    }

    @ToString
    private static class LeafPartition<T> implements Partition<T> {
        private final MappedObjectSequence<T> leaf;
        private final Comparator<? super T> comparator;

        public LeafPartition(MappedObjectSequence<T> leaf, Comparator<? super T> comparator) {
            assert leaf != null;
            assert leaf.size() <= 1;
            assert comparator != null;
            this.leaf = leaf;
            this.comparator = comparator;
        }

        @Override
        public int size() {
            return leaf.size();
        }

        @Override
        public boolean isEmpty() {
            return leaf.isEmpty();
        }

        @Override
        public T get(int index) {
            return leaf.get(index);
        }

        @Override
        public Spliterator<T> spliterator(int characteristics) {
            return new Spliterator<T>() {
                private int index = 0;

                @Override
                public boolean tryAdvance(Consumer<? super T> action) {
                    if (index == size())
                        return false;
                    action.accept(get(index++));
                    return true;
                }

                @Override
                public void forEachRemaining(Consumer<? super T> action) {
                    while (index < size())
                        action.accept(get(index++));
                }

                @Override
                public Spliterator<T> trySplit() {
                    return null;
                }

                @Override
                public long estimateSize() {
                    return size();
                }

                @Override
                public int characteristics() {
                    return characteristics;
                }

                @Override
                public Comparator<? super T> getComparator() {
                    if (!hasCharacteristics(Spliterator.SORTED))
                        throw new IllegalStateException();
                    return (Comparator<? super T>) comparator;
                }
            };
        }

        @Override
        public LeafPartition<T> reverse() {
            return new LeafPartition(leaf, reverseComparator(comparator));
        }
    }

    @RequiredArgsConstructor
    @ToString
    private static class MappedObjectSequence<T> {
        private final ObjectSequence<T> underlying;
        private final int[] mapping;

        public static <T> MappedObjectSequence<T> identity(ObjectSequence<T> seq) {
            return new MappedObjectSequence(seq, new ForwardSequence(0, seq.size()).toArray());
        }

        public MappedObjectSequence<T> remap(int[] newMapping) {
            int[] combinedMapping = new int[newMapping.length];
            for (int i = 0; i < combinedMapping.length; ++i) {
                int index = newMapping[i];
                assert (index >= 0 && index < mapping.length);
                combinedMapping[i] = mapping[index];
            }

            return new MappedObjectSequence(underlying, combinedMapping);
        }

        public int size() {
            return mapping.length;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public T get(int index) {
            if (index < 0 || index >= mapping.length)
                throw new NoSuchElementException();
            return underlying.get(mapping[index]);
        }
    }
}
