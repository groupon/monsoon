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
}
