package com.glencoesoftware.omero.ms.image.region;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import io.tiledb.java.api.Datatype;

public class TiledbUtils {

    private static final org.slf4j.Logger log =
        LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    public static byte getMaxByte(byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Cannot get max of empty array");
        } else {
            byte max = bytes[0];
            for(int i = 1; i < bytes.length; i++) {
                if (bytes[i] > max) {
                    max = bytes[i];
                }
            }
            return max;
        }
    }

    public static int getMaxUnsignedByte(byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Cannot get max of empty array");
        } else {
            int max = bytes[0] & 0xff;
            for(int i = 1; i < bytes.length; i++) {
                int val = bytes[i] & 0xff;
                if (val > max) {
                    max = val;
                }
            }
            return max;
        }
    }

    public static int getMaxUnsignedShort(byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Cannot get max of empty array");
        } else {
            ByteBuffer bbuf = ByteBuffer.wrap(bytes);
            ShortBuffer sbuf = bbuf.asShortBuffer();
            int max = sbuf.get() & 0xffff;
            while(sbuf.hasRemaining()) {
                int val = sbuf.get() & 0xffff;
                if(val > max) {
                    max = val;
                }
            }
            log.info("Max is: " + Integer.toString(max));
            return max;
        }
    }

    public static List<Integer> getMinMax(ByteBuffer buf, Datatype type) {
        switch(type) {
            case TILEDB_UINT8:
            case TILEDB_INT8: {
                byte min = (byte) 0;
                byte max = (byte) 0;
                while (buf.hasRemaining()) {
                    byte next = buf.get();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                List<Integer> ret = new ArrayList<Integer>();
                ret.add((int) min);
                ret.add((int) max);
                return ret;
            }
            case TILEDB_UINT16:
            case TILEDB_INT16: {
                short min = (short) 0;
                short max = (short) 0;
                while (buf.hasRemaining()) {
                    short next = buf.getShort();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                List<Integer> ret = new ArrayList<Integer>();
                ret.add((int) min);
                ret.add((int) max);
                return ret;
            }
            case TILEDB_UINT32:
            case TILEDB_INT32: {
                int min = (int) 0;
                int max = (int) 0;
                while (buf.hasRemaining()) {
                    int next = buf.getInt();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                List<Integer> ret = new ArrayList<Integer>();
                ret.add(min);
                ret.add(max);
                return ret;
            }
            default:
                throw new IllegalArgumentException("Type: " + type.toString() + " not supported");
        }
    }
}
