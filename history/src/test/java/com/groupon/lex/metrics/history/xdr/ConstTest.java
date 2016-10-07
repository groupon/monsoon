/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import java.io.IOException;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ConstTest {
    private final int VERSION = Const.version_from_majmin(Const.MAJOR, Const.MINOR);
    private tsfile_mimeheader hdr;

    @Before
    public void setup() {
        hdr = new tsfile_mimeheader();
        hdr.magic = Arrays.copyOf(Const.MAGIC, Const.MAGIC.length);
        hdr.version_number = VERSION;
    }

    @Test
    public void valid_header() {
        assertEquals(Const.Validation.CURRENT, Const.validateHeader(hdr));
    }

    @Test
    public void bad_magic() {
        hdr.magic[0] = 'X';
        assertEquals(Const.Validation.INVALID_MAGIC, Const.validateHeader(hdr));
    }

    @Test
    public void future_minor() {
        hdr.version_number = VERSION + 1;
        assertEquals(Const.Validation.NEW_MINOR, Const.validateHeader(hdr));
    }

    @Test
    public void future_major() {
        hdr.version_number = VERSION + 0x10000;
        assertEquals(Const.Validation.NEW_MAJOR, Const.validateHeader(hdr));
    }

    @Test
    @Ignore // enable once MINOR bump happens
    public void past_minor() {
        hdr.version_number = VERSION - 1;
        assertEquals(Const.Validation.OLD_MINOR, Const.validateHeader(hdr));
    }

    @Test
    @Ignore // enable once MAJOR bump happens
    public void past_major() {
        hdr.version_number = VERSION - 0x10000;
        assertEquals(Const.Validation.OLD_MAJOR, Const.validateHeader(hdr));
    }

    @Test
    public void validation_consts() {
        assertTrue(Const.Validation.CURRENT.isSameMajor());
        assertTrue(Const.Validation.CURRENT.isSameMinor());
        assertTrue(Const.Validation.CURRENT.isAcceptable());

        assertFalse(Const.Validation.NEW_MAJOR.isSameMajor());
        assertFalse(Const.Validation.NEW_MAJOR.isSameMinor());
        assertFalse(Const.Validation.NEW_MAJOR.isAcceptable());

        assertFalse(Const.Validation.OLD_MAJOR.isSameMajor());
        assertFalse(Const.Validation.OLD_MAJOR.isSameMinor());
        assertFalse(Const.Validation.OLD_MAJOR.isAcceptable());

        assertTrue(Const.Validation.NEW_MINOR.isSameMajor());
        assertFalse(Const.Validation.NEW_MINOR.isSameMinor());
        assertFalse(Const.Validation.NEW_MINOR.isAcceptable());

        assertTrue(Const.Validation.OLD_MINOR.isSameMajor());
        assertFalse(Const.Validation.OLD_MINOR.isSameMinor());
        assertTrue(Const.Validation.OLD_MINOR.isAcceptable());
    }

    @Test
    public void is_upgradable() {
        assertTrue(Const.isUpgradable(VERSION));
        if (Const.MINOR != 0)
            assertTrue(Const.isUpgradable(VERSION - 1));  // old MINOR, enable once MINOR bump happens
        assertFalse(Const.isUpgradable(VERSION + 1));  // new MINOR
        if (Const.MAJOR != 0)
            assertFalse(Const.isUpgradable(VERSION - 0x10000));  // old MAJOR, enable once MAJOR bump happens
        assertFalse(Const.isUpgradable(VERSION + 0x10000));  // new MAJOR
    }

    @Test
    public void happy_validate_or_throw() throws Exception {
        assertEquals(VERSION, Const.validateHeaderOrThrow(hdr));
    }

    @Test(expected = IOException.class)
    public void unhappy_validate_or_throw() throws Exception {
        hdr.magic[0] = 'X';
        Const.validateHeaderOrThrow(hdr);
    }

    @Test
    @Ignore  // Enable when the first MINOR bump happens.
    public void happy_validate_or_throw_for_write() throws Exception {
        assertFalse(Const.validateHeaderOrThrowForWrite(hdr));
        hdr.version_number = VERSION - 1;  // old MINOR
        assertTrue(Const.validateHeaderOrThrowForWrite(hdr));
    }

    @Test(expected = IOException.class)
    public void unhappy_validate_or_throw_for_write() throws Exception {
        hdr.version_number = VERSION + 0x10000;  // new MAJOR
        Const.validateHeaderOrThrowForWrite(hdr);
    }
}
