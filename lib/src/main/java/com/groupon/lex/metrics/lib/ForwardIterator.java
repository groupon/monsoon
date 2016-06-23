package com.groupon.lex.metrics.lib;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ForwardIterator<T> implements Iterator<T>, Cloneable {
    private static class Chain<T> {
        private final T elem_;  // The element in the chain.
        private Iterator<T> forward_;  // Iterator to advance one position, set to null after advancing the chain.
        private Chain<T> next_ = null;  // Next element in the chain, null until advanced.

        public Chain(T elem, Iterator<T> forward) {
            elem_ = elem;
            forward_ = forward;
        }

        public synchronized boolean hasNext() {
            return next_ != null || forward_.hasNext();
        }

        /**
         * Advance along the chain.
         * @return The next chain element, or null if no more elements are available.
         */
        public synchronized Chain<T> advance() {
            if (next_ == null) {
                if (!forward_.hasNext()) throw new NoSuchElementException();
                next_ = new Chain<>(forward_.next(), forward_);
                forward_ = null;
            }
            return next_;
        }

        /** Returns the current element. */
        public T get() { return elem_; }
    }

    private Chain<T> next_;

    public ForwardIterator(Iterator<T> iter) {
        this(new Chain<>(null, iter));
    }

    private ForwardIterator(Chain<T> next) {
        next_ = next;
    }

    @Override
    public boolean hasNext() {
        return next_.hasNext();
    }

    @Override
    public T next() {
        next_ = next_.advance();
        return next_.get();
    }

    @Override
    public ForwardIterator<T> clone() {
        return new ForwardIterator<>(next_);
    }
}
