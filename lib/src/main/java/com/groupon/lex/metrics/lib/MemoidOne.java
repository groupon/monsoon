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
package com.groupon.lex.metrics.lib;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * A trivial Memoid, that recalls the most recent calculation.
 * @author ariane
 */
public class MemoidOne<Input, Output> implements Function<Input, Output> {
    /**
     * Stored data.
     * @param <Input> The input type.
     * @param <Output> The output type.
     */
    private static class Data<Input, Output> {
        private final Input input_;
        private final Output output_;

        public Data(Input input, Output output) {
            input_ = Objects.requireNonNull(input);
            output_ = output;
        }

        public Input getInput() { return input_; }
        public Output getOutput() { return output_; }
    }

    /** Store most recent calculation. */
    private final AtomicReference<Data<Input, Output>> recent_ = new AtomicReference<>();
    /** Function that will perform a calculation. */
    private final Function<? super Input, ? extends Output> fn_;
    /** Comparison predicate. */
    private final BiPredicate<Input, Input> equality_;

    /**
     * Create a new MemoidOne, using the supplied transformation function.
     * @param fn The function that backs this.
     */
    public MemoidOne(Function<? super Input, ? extends Output> fn) {
        this(fn, Objects::deepEquals);
    }

    /**
     * Create a new MemoidOne, using the supplied transformation function.
     * @param fn The function that backs this.
     * @param equality The equality predicate, used to determine if two inputs are equal.
     */
    public MemoidOne(Function<? super Input, ? extends Output> fn, BiPredicate<Input, Input> equality) {
        fn_ = Objects.requireNonNull(fn);
        equality_ = Objects.requireNonNull(equality);
    }

    /**
     * Try to match an input against the most recent calculation.
     * @param input The most recent input.
     * @return An optional Data object, if the input matches the remembered calculation.
     */
    private Optional<Data<Input, Output>> match_(Input input) {
        return Optional.ofNullable(recent_.get())
                .filter((Data<Input, Output> data) -> equality_.test(data.getInput(), input));
    }

    /**
     * Derive and remember a calculation.
     *
     * Future invocations of match_(input) will return the returned Data instance.
     * @param input The input for the calculation.
     * @return A data object, holding the result of the calculation.
     */
    private Data<Input, Output> derive_(Input input) {
        Data<Input, Output> result = new Data<>(input, fn_.apply(input));
        recent_.set(result);
        return result;
    }

    /**
     * Functional implementation.
     *
     * The actual calculation will only be performed if the input doesn't match
     * the input from the most recent invocation.
     *
     * Null inputs are never remembered.
     * @param input Argument to the calculation.
     * @return The output of the calculation.
     */
    @Override
    public Output apply(Input input) {
        if (input == null) return fn_.apply(input);
        return match_(input)
                .orElseGet(() -> derive_(input))
                .getOutput();
    }
}
