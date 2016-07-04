/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr.support;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A Collection that wraps and exposes a stream.
 *
 * The collection requires that the underlying stream can be recreated with the
 * same values each time.
 * It lazily instantiates the elements in the list, when iterated over.
 *
 * It allows most of the standard Stream operations, except for the ones that
 * convert to specialized streams, such as IntStream.  Since streams are immutable,
 * this collection is also immutable.
 *
 * It is used to keep track of large collections, without spending a lot of
 * memory on it.  Since the elements in the tests are always deterministically
 * created, they can be recreated when requested.
 *
 * Note: the stream operations modify the internal state of this collection.
 * You won't get a copy of the stream with different parameters.
 */
public class StreamedCollection<T> extends AbstractCollection<T> implements Collection<T>, Stream<T> {
    private final Collection<Runnable> closeHandlers;
    private Supplier<Stream<T>> stream;

    private StreamedCollection(Supplier<Stream<T>> stream, Collection<Runnable> close_handlers) {
        this.stream = Objects.requireNonNull(stream);
        closeHandlers = Objects.requireNonNull(close_handlers);
    }

    public StreamedCollection(Supplier<Stream<T>> stream) {
        this(stream, new ArrayList<>());
    }

    @Override
    public Iterator<T> iterator() {
        return stream.get().iterator();
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().filter(predicate);
        return this;
    }

    @Override
    public <R> StreamedCollection<R> map(Function<? super T, ? extends R> mapper) {
        final Supplier<Stream<T>> nested = stream;
        return new StreamedCollection<R>(() -> nested.get().map(mapper), closeHandlers);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <R> StreamedCollection<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        final Supplier<Stream<T>> nested = stream;
        return new StreamedCollection<R>(() -> nested.get().flatMap(mapper), closeHandlers);
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StreamedCollection<T> distinct() {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().distinct();
        return this;
    }

    @Override
    public StreamedCollection<T> sorted() {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().sorted();
        return this;
    }

    @Override
    public StreamedCollection<T> sorted(Comparator<? super T> comparator) {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().sorted(comparator);
        return this;
    }

    @Override
    public StreamedCollection<T> peek(Consumer<? super T> action) {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().peek(action);
        return this;
    }

    @Override
    public StreamedCollection<T> limit(long maxSize) {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().limit(maxSize);
        return this;
    }

    @Override
    public StreamedCollection<T> skip(long n) {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().skip(n);
        return this;
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        try (final Stream<T> s = stream.get()) {
            s.forEach(action);
        }
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        try (final Stream<T> s = stream.get()) {
            s.forEachOrdered(action);
        }
    }

    @Override
    public Object[] toArray() {
        try (final Stream<T> s = stream.get()) {
            return s.toArray();
        }
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        try (final Stream<T> s = stream.get()) {
            return s.toArray(generator);
        }
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        try (final Stream<T> s = stream.get()) {
            return s.reduce(identity, accumulator);
        }
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        try (final Stream<T> s = stream.get()) {
            return s.reduce(accumulator);
        }
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        try (final Stream<T> s = stream.get()) {
            return s.reduce(identity, accumulator, combiner);
        }
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        try (final Stream<T> s = stream.get()) {
            return s.collect(supplier, accumulator, combiner);
        }
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        try (final Stream<T> s = stream.get()) {
            return s.collect(collector);
        }
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        try (final Stream<T> s = stream.get()) {
            return s.min(comparator);
        }
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        try (final Stream<T> s = stream.get()) {
            return s.max(comparator);
        }
    }

    @Override
    public long count() {
        try (final Stream<T> s = stream.get()) {
            return s.count();
        }
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        try (final Stream<T> s = stream.get()) {
            return s.anyMatch(predicate);
        }
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        try (final Stream<T> s = stream.get()) {
            return s.allMatch(predicate);
        }
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        try (final Stream<T> s = stream.get()) {
            return s.noneMatch(predicate);
        }
    }

    @Override
    public Optional<T> findFirst() {
        try (final Stream<T> s = stream.get()) {
            return s.findFirst();
        }
    }

    @Override
    public Optional<T> findAny() {
        try (final Stream<T> s = stream.get()) {
            return s.findAny();
        }
    }

    @Override
    public Spliterator<T> spliterator() {
        try (final Stream<T> s = stream.get()) {
            return s.spliterator();
        }
    }

    @Override
    public boolean isParallel() {
        try (final Stream<T> s = stream.get()) {
            return s.isParallel();
        }
    }

    @Override
    public Stream<T> sequential() {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().sequential();
        return this;
    }

    @Override
    public Stream<T> parallel() {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().parallel();
        return this;
    }

    @Override
    public Stream<T> unordered() {
        final Supplier<Stream<T>> nested = stream;
        stream = () -> nested.get().unordered();
        return this;
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        closeHandlers.add(closeHandler);
        return this;
    }

    @Override
    public void close() {
        closeHandlers.forEach(Runnable::run);
    }

    @Override
    public int size() {
        return (int) count();
    }

    /**
     * List equality: compare each element in sequence for equality.
     *
     * The equality for a list is defined in the documentation of the List interface:
     * https://docs.oracle.com/javase/7/docs/api/java/util/List.html#equals%28java.lang.Object%29
     *
     * Note that Lombok won't generate the correct equals() and hashCode(),
     * as it will compare the members of the object, none of which has a defined
     * equality and thus will never compare equal.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Collection)) {
            return false;
        }
        final Collection<?> other = (Collection<?>) o;
        final Iterator<?> self_iter = iterator();
        final Iterator<?> othr_iter = other.iterator();
        while (self_iter.hasNext() && othr_iter.hasNext()) {
            if (!Objects.equals(self_iter.next(), othr_iter.next()))
                return false;
        }
        return !(self_iter.hasNext() || othr_iter.hasNext());
    }

    /**
     * List hashCode: the hashcode of a list is a function of each of its elements.
     *
     * The hashCode for a list is defined in the documentation of the List interface:
     * https://docs.oracle.com/javase/7/docs/api/java/util/List.html#hashCode%28%29
     *
     * Note that Lombok won't generate the correct equals() and hashCode(),
     * as it will compare the members of the object, none of which has a defined
     * equality and thus will never compare equal.
     */
    @Override
    public int hashCode() {
        int result = 1;
        final Iterator<?> iter = iterator();
        while (iter.hasNext())
            result = 31 * result + Objects.hashCode(iter.next());
        return result;
    }
}
