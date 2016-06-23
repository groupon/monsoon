package com.groupon.lex.metrics.history.xdr.support;

/**
 *
 * @author ariane
 */
public class GzipHeaderConsts {
    private GzipHeaderConsts() {}  // Not constructible.

    public final static byte ID1_EXPECT = 0x1f;
    public final static byte ID2_EXPECT = -0x75; // 0x8b, but since java doesn't have unsigned data types...
}
