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
package com.groupon.lex.metrics.builders.collector;

import com.groupon.lex.metrics.GroupGenerator;
import java.util.function.BiConsumer;
import javax.servlet.http.HttpServlet;

/**
 * Builder interface for a collector.
 *
 * An implementation of this class must:
 * <ol>
 * <li>implement at least one of the Main* classes: MainNone, MainString or
 * MainStringList</li>
 * </ol>
 *
 * An implementation of this class may:
 * <ol>
 * <li>implement one of AcceptAsPath, AcceptOptAsPath</li>
 * <li>implement AcceptTagSet</li>
 * </ol>
 *
 * These extra interfaces are used to instruct the parser to handle the
 * collector statements appropriately.
 *
 * @author ariane
 */
public interface CollectorBuilder {
    /**
     * Create the actual collector.
     *
     * The parser will call each of the setters from the Main* and Accept* base
     * classes of the implementation exactly once, before calling this function.
     * The function must be able to return multiple instances of the collector.
     *
     * @param er Endpoint registration API. Used for registering HTTP endpoints.
     * @return A GroupGenerator implementation that performs the desired
     * collection.
     * @throws Exception If the implementation fails to create the collector.
     */
    public GroupGenerator build(BiConsumer<String, HttpServlet> er) throws Exception;
}
