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
package com.groupon.lex.metrics.history.xdr.support;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import java.security.SecureRandom;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 *
 * @author ariane
 */
public class FileUtil {
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Create a temporary file that will be removed when it is closed. */
    public static FileChannel createTempFile(Path dir, String prefix, String suffix) throws IOException {
        return createNewFileImpl(dir, prefix, suffix, new OpenOption[]{ READ, WRITE, CREATE_NEW, DELETE_ON_CLOSE }).getFileChannel();
    }

    /** Creates a new file and opens it for reading and writing. */
    public static NamedFileChannel createNewFile(Path dir, String prefix, String suffix) throws IOException {
        return createNewFileImpl(dir, prefix, suffix, new OpenOption[]{ READ, WRITE, CREATE_NEW });
    }

    private static NamedFileChannel createNewFileImpl(Path dir, String prefix, String suffix, OpenOption openOptions[]) throws IOException {
        try {
            final Path new_filename = dir.resolve(prefix + suffix);
            return new NamedFileChannel(new_filename, FileChannel.open(new_filename, openOptions));
        } catch (IOException ex) {
            /* Ignore. */
        }

        /*
         * Try with random number as differentiator between filenames.
         */
        for (int i = 0; i < 15; ++i) {
            long idx = RANDOM.nextLong() & 0x7fffffffffffffffL;  // Mask, to remove negative numbers.
            final Path new_filename = dir.resolve(String.format("%s-%d%s", prefix, idx, suffix));
            try {
                return new NamedFileChannel(new_filename, FileChannel.open(new_filename, openOptions));
            } catch (IOException ex) {
                /* Ignore. */
            }
        }

        /*
         * Try one more time with differentiator, but let the exception out this time.
         */
        long idx = RANDOM.nextLong() & 0x7fffffffffffffffL;  // Mask, to remove negative numbers.
        final Path new_filename = dir.resolve(String.format("%s-%d.tsd", prefix, idx));
        return new NamedFileChannel(new_filename, FileChannel.open(new_filename, openOptions));
    }

    @AllArgsConstructor
    @Getter
    public static class NamedFileChannel {
        private final Path fileName;
        private final FileChannel fileChannel;
    }
}
