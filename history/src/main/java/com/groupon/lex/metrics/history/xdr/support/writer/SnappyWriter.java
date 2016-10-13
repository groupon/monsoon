/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr.support.writer;

import java.io.IOException;
import lombok.NonNull;
import org.iq80.snappy.SnappyFramedOutputStream;

/**
 *
 * @author ariane
 */
public class SnappyWriter extends AbstractOutputStreamWriter {
    public SnappyWriter(@NonNull FileWriter out) throws IOException {
        super(new SnappyFramedOutputStream(newAdapter(out)));
    }
}
