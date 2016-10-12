/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author ariane
 */
public interface BufferSupplier {
    public void load(ByteBuffer buf) throws IOException;
    public boolean atEof() throws IOException;

    public static BufferSupplier singleBuffer(ByteBuffer view) {
        return new BufferSupplier() {
            @Override
            public void load(ByteBuffer buf) throws IOException {
                if (view.hasRemaining() && buf.hasRemaining()) {
                    if (view.remaining() <= buf.remaining()) {
                        buf.put(view);
                    } else {
                        final ByteBuffer tmp = view.duplicate();
                        tmp.limit(tmp.position() + buf.remaining());
                        view.position(view.position() + tmp.remaining());
                        buf.put(tmp);
                    }
                }
            }

            @Override
            public boolean atEof() throws IOException {
                return !view.hasRemaining();
            }
        };
    }
}
