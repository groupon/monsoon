package com.groupon.lex.metrics.api.endpoints;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import com.groupon.lex.metrics.lib.BufferedIterator;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import static java.util.Objects.requireNonNull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

/**
 *
 * @author ariane
 */
public class StreamingJsonListEntity<T> implements WriteListener {
    private static final Logger LOG = Logger.getLogger(StreamingJsonListEntity.class.getName());
    private static final ExecutorService work_queue_ = Executors.newFixedThreadPool(Integer.max(Runtime.getRuntime().availableProcessors() - 1, 2));
    private static final Gson gson_ = new GsonBuilder().setPrettyPrinting().create();
    private final BufferedIterator<T> iter_;
    private final JsonWriter writer_;
    private final ServletOutputStream out_;
    private final AsyncContext ctx_;

    public StreamingJsonListEntity(AsyncContext ctx, ServletOutputStream out, Iterator<T> iter) throws IOException {
        ctx_ = requireNonNull(ctx);
        out_ = requireNonNull(out);
        iter_ = new BufferedIterator<>(work_queue_, iter);
        ctx_.setTimeout(300000);
        try {
            writer_ = gson_.newJsonWriter(new OutputStreamWriter(new NonClosingOutputStreamWrapper(out), "UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            LOG.log(Level.SEVERE, "UTF-8 is unsupported?!", ex);
            throw new IOException("UTF-8 encoding is not supported", ex);
        }

        writer_.beginArray();
    }

    public StreamingJsonListEntity(AsyncContext ctx, ServletOutputStream out, Stream<T> stream) throws IOException {
        this(ctx, out, stream.iterator());
    }

    public StreamingJsonListEntity(AsyncContext ctx, ServletOutputStream out, Iterable<T> iterable) throws IOException {
        this(ctx, out, iterable.iterator());
    }

    @Override
    public void onWritePossible() {
        if (!out_.isReady()) return;

        try {
            if (iter_.nextAvail()) {
                final T v = iter_.next();
                if (v != null)
                    gson_.toJson(v, v.getClass(), writer_);
                else
                    writer_.nullValue();
            }

            if (iter_.atEnd()) {
                writer_.endArray();
                writer_.close();
                ctx_.complete();
                return;
            }

            iter_.setWakeup(this::onWritePossible);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "error while streaming json response", ex);
            ctx_.complete();
        }
    }

    @Override
    public void onError(Throwable t) {
        ctx_.complete();
    }

    private static class NonClosingOutputStreamWrapper extends OutputStream {
        private final OutputStream underlying_;

        public NonClosingOutputStreamWrapper(OutputStream underlying) {
            underlying_ = requireNonNull(underlying);
        }

        @Override
        public void write(int b) throws IOException { underlying_.write(b); }
        @Override
        public void write(byte b[]) throws IOException { underlying_.write(b); }
        @Override
        public void write(byte b[], int off, int len) throws IOException { underlying_.write(b, off, len); }
        @Override
        public void flush() throws IOException {}
        @Override
        public void close() throws IOException {}
    }
}
