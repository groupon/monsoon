/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;
import org.acplt.oncrpc.XdrEncodingStream;

/**
 *
 * @author ariane
 */
public class Const {
    private static final Logger LOG = Logger.getLogger(Const.class.getName());
    private Const() {}

    public static byte[] MAGIC = new byte[]{  17,  19,  23,  29,
                                             'M', 'O', 'N', '-',
                                             's', 'o', 'o', 'n' };  // 12 chars
    public static final int MIME_HEADER_LEN = 16;  // Mime header is 16 bytes.
    public static final short MAJOR = 1;
    public static final short MINOR = 0;

    public static int version_from_majmin(short maj, short min) {
        if (maj < 0 || min < 0) throw new IllegalArgumentException("Java needs unsigned data types!");
        return (int)maj << 16 | (int)min;
    }

    public static short version_major(int ver) {
        if (ver < 0) throw new IllegalArgumentException("Java needs unsigned data types!");
        return (short)(ver >> 16);
    }

    public static short version_minor(int ver) {
        if (ver < 0) throw new IllegalArgumentException("Java needs unsigned data types!");
        return (short)(ver & 0xffff);
    }

    public static enum Validation {
        /** File is written with older major version. */
        OLD_MAJOR(-1, -1),
        /** File is written with older minor version, but same major version. */
        OLD_MINOR(0, -1),
        /** File is written with current version. */
        CURRENT(0, 0),
        /** File is written with newer minor version. */
        NEW_MINOR(0, 1),
        /** File is written with newer major version. */
        NEW_MAJOR(1, 1),
        /** File is not a valid tsdata file, because magic doesn't match. */
        INVALID_MAGIC(Integer.MAX_VALUE, Integer.MAX_VALUE),
        /** File is not a valid tsdata file, because version number is negative. */
        INVALID_NEG_VERSION(Integer.MAX_VALUE, Integer.MAX_VALUE);

        private final int same_major_;
        private final int same_minor_;

        private Validation(int same_major, int same_minor) {
            same_major_ = same_major;
            same_minor_ = same_minor;
        }

        public boolean isSameMajor() { return same_major_ == 0; }
        public boolean isSameMinor() { return same_minor_ == 0; }
        public boolean isAcceptable() { return isSameMajor() && same_minor_ <= 0; }
        public boolean isReadable() { return same_major_ < 0 || isAcceptable(); }
    }

    public static boolean isUpgradable(short maj, short min) {
        return (maj == MAJOR && min <= MINOR);
    }

    public static boolean isUpgradable(int version) {
        return isUpgradable(version_major(version), version_minor(version));
    }

    public static boolean needsUpgrade(short maj, short min) {
        return isUpgradable(maj, min) && (maj != MAJOR || min != MINOR);
    }

    public static boolean needsUpgrade(int version) {
        return needsUpgrade(version_major(version), version_minor(version));
    }

    public static Validation validateHeader(tsfile_mimeheader hdr) {
        LOG.log(Level.INFO, "mimeheader: {0}", mimeHexdump(hdr.magic));
        if (!Arrays.equals(MAGIC, hdr.magic)) return Validation.INVALID_MAGIC;
        if (hdr.version_number < 0) return Validation.INVALID_NEG_VERSION;
        int maj_cmp = Short.compare(version_major(hdr.version_number), MAJOR);
        int min_cmp = Short.compare(version_minor(hdr.version_number), MINOR);
        if (maj_cmp != 0) return (maj_cmp < 0 ? Validation.OLD_MAJOR : Validation.NEW_MAJOR);
        if (min_cmp != 0) return (min_cmp < 0 ? Validation.OLD_MINOR : Validation.NEW_MINOR);
        return Validation.CURRENT;
    }

    public static int validateHeaderOrThrow(tsfile_mimeheader hdr) throws IOException {
        if (!validateHeader(hdr).isReadable())
            throw new IOException("Can't read this file, header validation yields " + validateHeader(hdr).name() + "(" + versionStr(hdr.version_number) + ")");
        return hdr.version_number;
    }

    public static boolean validateHeaderOrThrowForWrite(tsfile_mimeheader hdr) throws IOException {
        if (!validateHeader(hdr).isAcceptable() || !isUpgradable(hdr.version_number))
            throw new IOException("Can't read this file, header validation yields " + validateHeader(hdr).name() + "(" + versionStr(hdr.version_number) + ")");
        return needsUpgrade(hdr.version_number);
    }

    public static int validateHeaderOrThrow(XdrDecodingStream decoder) throws IOException, OncRpcException {
        return validateHeaderOrThrow(new tsfile_mimeheader(decoder));
    }

    public static boolean validateHeaderOrThrowForWrite(XdrDecodingStream decoder) throws IOException, OncRpcException {
        return validateHeaderOrThrowForWrite(new tsfile_mimeheader(decoder));
    }

    public static void writeMimeHeader(XdrEncodingStream encoder, short major, short minor) throws IOException, OncRpcException {
        tsfile_mimeheader hdr = new tsfile_mimeheader();
        hdr.magic = MAGIC;
        hdr.version_number = version_from_majmin(major, minor);
        hdr.xdrEncode(encoder);
    }

    public static void writeMimeHeader(XdrEncodingStream encoder) throws IOException, OncRpcException {
        writeMimeHeader(encoder, MAJOR, MINOR);
    }

    private static String versionStr(int version) {
        int major = version_major(version);
        int minor = version_minor(version);
        major &= 0xffff;
        minor &= 0xffff;
        return "v" + major + "." + minor;
    }

    private static String mimeHexdump(byte data[]) {
        if (data.length == 0) return "(no data)";

        String result = new String();
        for (byte b : data) {
            int v = b;
            v &= 0xff;
            result += " " + Integer.toHexString(v);
        }
        return result.substring(1);
    }
}
