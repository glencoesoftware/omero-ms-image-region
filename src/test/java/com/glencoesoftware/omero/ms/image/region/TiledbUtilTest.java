package com.glencoesoftware.omero.ms.image.region;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.tiledb.java.api.Datatype;

public class TiledbUtilTest {

    @Test public void getSubarrayDomainStdTest() {
        long[] subArrayDomain = TiledbUtils.getSubarrayDomainFromString("[0,0,0,100:150,200:250]");
        Assert.assertTrue(Arrays.equals(subArrayDomain, new long[] {0,0,0,0,0,0,100,149,200,249}));
    }

    @Test public void minMaxTest() {
        byte[] testBytes = new byte[] {-128, 0, 127, -1};
        ByteBuffer testBuf = ByteBuffer.wrap(testBytes);
        testBuf.mark();
        long[] minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_INT8);
        Assert.assertTrue(Arrays.equals(minMax, new long[] {-128, 127}));
        testBuf.reset();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_UINT8);
        Assert.assertTrue(Arrays.equals(minMax, new long[] {0, 255}));

        short[] testShorts = new short[] {-32768, 0, 32767, -1};
        testBuf = ByteBuffer.allocate(testShorts.length*2);
        testBuf.asShortBuffer().put(testShorts);
        testBuf.mark();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_INT16);
        Assert.assertTrue(Arrays.equals(minMax, new long[] {-32768, 32767}));
        testBuf.reset();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_UINT16);
        Assert.assertTrue(Arrays.equals(minMax, new long[] {0, 65535}));

        int[] testInts = new int[] {-2147483648, 0, 2147483647, -1};
        testBuf = ByteBuffer.allocate(testInts.length*4);
        testBuf.asIntBuffer().put(testInts);
        testBuf.mark();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_INT32);
        Assert.assertTrue(Arrays.equals(minMax, new long[] {-2147483648, 2147483647}));
        testBuf.reset();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_UINT32);
        Assert.assertTrue(Arrays.equals(minMax, new long[] {0, 4294967295L}));
    }

}
