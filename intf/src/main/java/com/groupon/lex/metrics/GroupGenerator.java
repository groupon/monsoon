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
package com.groupon.lex.metrics;

import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Provides multiple groups.
 *
 * @author ariane
 */
public interface GroupGenerator extends AutoCloseable {
    /**
     * The result of a collection. Holds groups and an indicator of success.
     */
    @Deprecated
    public static final class GroupCollection {
        private final boolean successful_;
        private final Collection<? extends MetricGroup> groups_;

        private GroupCollection(boolean successful, Collection<? extends MetricGroup> groups) {
            successful_ = successful;
            groups_ = groups;
        }

        /**
         * Check if the collection was successful.
         *
         * @return True iff the collection was successful, false otherwise.
         */
        public boolean isSuccessful() {
            return successful_;
        }

        /**
         * Returns the groups collected by the collector.
         *
         * @return The groups collected by the collector if the collection
         * succeeded. An empty set is returned if the collection failed.
         */
        public Collection<? extends MetricGroup> getGroups() {
            return groups_;
        }
    }

    /**
     * Create a new success result.
     *
     * @param groups The groups collected.
     * @return A GroupCollection holding the collection.
     */
    public static GroupCollection successResult(Collection<? extends MetricGroup> groups) {
        return new GroupCollection(true, groups);
    }

    /**
     * Create a new failure result.
     *
     * @return A GroupCollection instance that indicates the collection failed.
     */
    public static GroupCollection failedResult() {
        return new GroupCollection(false, EMPTY_LIST);
    }

    public CompletableFuture<Collection<MetricGroup>> getGroups(ExecutorService executor, CompletableFuture<?> timeout);

    @Override
    default void close() throws Exception {
    }

    public static CompletableFuture<Collection<MetricGroup>> combineSubTasks(Collection<CompletableFuture<Collection<MetricGroup>>> subTasks) {
        return CompletableFuture.allOf(subTasks.toArray(new CompletableFuture[0]))
                .thenApply((ignored) -> {
                    try {
                        Collection<MetricGroup> combined = new ArrayList<>();
                        for (CompletableFuture<Collection<MetricGroup>> result : subTasks) {
                            combined.addAll(result.get());
                        }
                        return combined;
                    } catch (InterruptedException | ExecutionException ex) {
                        throw new RuntimeException(ex);  // Shouldn't happen, as this method is only called on successful return.
                    }
                });
    }

    public static CompletableFuture<Collection<MetricGroup>> combineGroups(Collection<CompletableFuture<MetricGroup>> subTasks) {
        return CompletableFuture.allOf(subTasks.toArray(new CompletableFuture[0]))
                .thenApply((ignored) -> {
                    try {
                        Collection<MetricGroup> combined = new ArrayList<>(subTasks.size());
                        for (CompletableFuture<MetricGroup> result : subTasks) {
                            combined.add(result.get());
                        }
                        return combined;
                    } catch (InterruptedException | ExecutionException ex) {
                        throw new RuntimeException(ex);  // Shouldn't happen, as this method is only called on successful return.
                    }
                });
    }
}
