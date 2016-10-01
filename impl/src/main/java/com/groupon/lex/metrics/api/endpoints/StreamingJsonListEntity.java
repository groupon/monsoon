package com.groupon.lex.metrics.api.endpoints;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import com.groupon.lex.metrics.lib.BufferedIterator;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class StreamingJsonListEntity<T> implements WriteListener {
    private static final Logger LOG = Logger.getLogger(StreamingJsonListEntity.class.getName());
    private static final Gson gson_ = new GsonBuilder().disableHtmlEscaping().create();
    private final BufferedIterator<T> iter_;
    private final JsonWriter writer_;
    private final ServletOutputStream out_;
    private final AsyncContext ctx_;
    private final Runnable onIterDone;
    private final Long deadline;  // May be null, in which case there is no deadline.
    private DateTime begin;
    private final Duration stepSize;
    private final Function<T, DateTime> valueToTime;

    public StreamingJsonListEntity(
            @NonNull AsyncContext ctx,
            @NonNull ServletOutputStream out,
            @NonNull BufferedIterator<T> iter,
            @NonNull String idx,
            @NonNull String cookie,
            @NonNull DateTime begin,
            @NonNull Duration stepSize,
            @NonNull Function<T, DateTime> valueToTime,
            @NonNull Runnable onIterDone,
            @NonNull Optional<Long> deadline) throws IOException {
        ctx_ = ctx;
        out_ = out;
        iter_ = iter;
        this.onIterDone = onIterDone;
        this.deadline = deadline.orElse(null);
        this.begin = begin;
        this.valueToTime = valueToTime;
        this.stepSize = stepSize;

        ctx_.setTimeout(300000);
        try {
            writer_ = gson_.newJsonWriter(new OutputStreamWriter(new NonClosingOutputStreamWrapper(out), "UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            LOG.log(Level.SEVERE, "UTF-8 is unsupported?!", ex);
            throw new IOException("UTF-8 encoding is not supported", ex);
        }

        writer_.beginObject();
        writer_.name("iter").value(idx);
        writer_.name("cookie").value(cookie);
        writer_.name("data").beginArray();
    }

    @Override
    public void onWritePossible() {
        if (!out_.isReady()) return;

        try {
            if (iter_.nextAvail()) {
                final T v = iter_.next();
                if (v != null) {
                    gson_.toJson(v, v.getClass(), writer_);
                    begin = valueToTime.apply(v).toDateTime(DateTimeZone.UTC).plus(stepSize);
                } else {
                    writer_.nullValue();
                }
            }

            if (iter_.atEnd()) {
                finishTx(true);
                onIterDone.run();
                return;
            }

            if (deadline == null) {
                iter_.setWakeup(this::onWritePossible);
            } else {
                final long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0)
                    finishTx(false);
                else
                    iter_.setWakeup(this::onWritePossible, remaining, TimeUnit.MILLISECONDS);
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "error while streaming json response", ex);
            ctx_.complete();
        }
    }

    @Override
    public void onError(Throwable t) {
        ctx_.complete();
    }

    private void finishTx(boolean last) throws IOException {
        writer_.endArray();
        writer_.name("last").value(last);
        writer_.name("newBegin").value(begin.getMillis());
        writer_.endObject();
        writer_.close();
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
