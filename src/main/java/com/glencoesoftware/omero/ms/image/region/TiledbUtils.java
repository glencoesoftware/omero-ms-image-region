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

    public static int getMinUnsignedByte(byte[] bytes) {
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
            return max;
        }
    }

    public static int getMinUnsignedShort(byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Cannot get max of empty array");
        } else {
            ByteBuffer bbuf = ByteBuffer.wrap(bytes);
            ShortBuffer sbuf = bbuf.asShortBuffer();
            int min = sbuf.get() & 0xffff;
            while(sbuf.hasRemaining()) {
                int val = sbuf.get() & 0xffff;
                if(val < min) {
                    min = val;
                }
            }
            return min;
        }
    }

    public static long[] getMinMax(ByteBuffer buf, Datatype type) {
        if (!buf.hasRemaining()) {
            throw new IllegalArgumentException("Cannot get max of empty buffer");
        }
        switch(type) {
            case TILEDB_UINT8: {
                long max = buf.get() & 0xff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.get() & 0xff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case TILEDB_INT8: {
                byte min = (byte) 0;
                byte max = (byte) 0;
                while (buf.hasRemaining()) {
                    byte next = buf.get();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case TILEDB_UINT16: {
                long max = buf.getShort() & 0xffff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getShort() & 0xffff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case TILEDB_INT16: {
                short min = (short) 0;
                short max = (short) 0;
                while (buf.hasRemaining()) {
                    short next = buf.getShort();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case TILEDB_UINT32:{
                long max = buf.getInt() & 0xffffffff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getInt() & 0xffffffff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case TILEDB_INT32: {
                int min = 0;
                int max = 0;
                while (buf.hasRemaining()) {
                    int next = buf.getInt();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            default:
                throw new IllegalArgumentException("Type: " + type.toString() + " not supported");
        }
    }

    public static int getBytesPerPixel(Datatype type) {
        switch (type) {
            case TILEDB_UINT8:
            case TILEDB_INT8:
                return 1;
            case TILEDB_UINT16:
            case TILEDB_INT16:
                return 2;
            case TILEDB_UINT32:
            case TILEDB_INT32:
                return 4;
            case TILEDB_UINT64:
            case TILEDB_INT64:
                return 8;
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    public static long[] getSubarrayDomainFromString(String domainStr) {
        //String like [0,1,0,100:150,200:250]
        if(domainStr.length() == 0) {
            return null;
        }
        if(domainStr.startsWith("[")) {
            domainStr = domainStr.substring(1);
        }
        if(domainStr.endsWith("]")) {
            domainStr = domainStr.substring(0, domainStr.length() - 1);
        }
        String[] dimStrs = domainStr.split(",");
        if(dimStrs.length != 5) {
            throw new IllegalArgumentException("Invalid number of dimensions in domain string");
        }
        long[] subarrayDomain = new long[5*2];
        for(int i = 0; i < 5; i++) {
            String s = dimStrs[i];
            if(s.contains(":")) {
                String[] startEnd = s.split(":");
                subarrayDomain[i*2] = Long.valueOf(startEnd[0]);
                subarrayDomain[i*2 + 1] = Long.valueOf(startEnd[1]) - 1;
            } else {
                subarrayDomain[i*2] = Long.valueOf(s);
                subarrayDomain[i*2 + 1] = Long.valueOf(s);
            }
        }
        return subarrayDomain;
    }
}
