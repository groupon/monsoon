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
package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.history.v2.TSDataFactory2;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import static com.groupon.lex.metrics.history.xdr.Const.version_major;
import static com.groupon.lex.metrics.history.xdr.Const.version_minor;
import com.groupon.lex.metrics.history.xdr.MmapReadonlyTSDataFile;
import com.groupon.lex.metrics.history.xdr.UnmappedReadonlyTSDataFile;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.acplt.oncrpc.OncRpcException;

/**
 *
 * @author ariane
 */
public class TSDataVersionDispatch {
    public static final List<Factory> VERSION_TABLE = unmodifiableList(Arrays.asList(
            new TSData_0_and_1(), // version 0
            new TSData_0_and_1(), // version 1
            new TSDataFactory2()  // version 2
    ));

    public static interface Factory {
        public TSData open(Releaseable<FileChannel> file) throws IOException;
    }

    public static TSData open(Path file) throws IOException {
        try (Releaseable<FileChannel> fd = new Releaseable<>(FileChannel.open(file, StandardOpenOption.READ))) {
            final int version;
            try (XdrDecodingFileReader reader = new XdrDecodingFileReader(new FileChannelReader(fd.get(), 0), MIME_HEADER_LEN)) {
                reader.beginDecoding();
                version = validateHeaderOrThrow(reader);
                reader.endDecoding();
            } catch (OncRpcException ex) {
                throw new IOException("decoding error while reading mime header", ex);
            }

            final short majorVersion = version_major(version);
            final Factory factory;
            try {
                factory = VERSION_TABLE.get(majorVersion);
                if (factory == null) throw new IndexOutOfBoundsException("null decoder");
            } catch (IndexOutOfBoundsException ex) {
                throw new IOException("missing implementation for version " + majorVersion + "." + version_minor(version));
            }

            return factory.open(fd);
        }
    }

    @AllArgsConstructor
    public static class Releaseable<T extends Closeable> implements Closeable {
        private T item;

        public T get() {
            return item;
        }

        public T release() {
            T result = item;
            item = null;
            return result;
        }

        @Override
        public void close() throws IOException {
            if (item != null) item.close();
        }
    }

    private static class TSData_0_and_1 implements Factory {
        private static final int MIN_MMAP_SIZE =  1 * 1024 * 1024;
        private static final int MAX_MMAP_SIZE = 32 * 1024 * 1024;

        @Override
        public TSData open(Releaseable<FileChannel> fd) throws IOException {
            if (fd.get().size() >= MIN_MMAP_SIZE && fd.get().size() <= MAX_MMAP_SIZE)
                return new MmapReadonlyTSDataFile(fd.get().map(FileChannel.MapMode.READ_ONLY, 0, fd.get().size()));
            else
                return new UnmappedReadonlyTSDataFile(new GCCloseable<>(fd.release()));
        }
    }
}
