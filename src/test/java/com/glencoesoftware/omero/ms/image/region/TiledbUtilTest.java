/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.glencoesoftware.omero.ms.image.region;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.tiledb.java.api.Datatype;

public class TiledbUtilTest {

    @Test public void getSubarrayDomainStdTest() {
        long[] subArrayDomain = TiledbUtils.getSubarrayDomainFromString(
                "[0,0,0,100:150,200:250]");
        Assert.assertTrue(Arrays.equals(
                subArrayDomain, new long[] {0,0,0,0,0,0,100,149,200,249}));
    }

    @Test public void minMaxTest() {
        byte[] testBytes = new byte[] {-128, 0, 127, -1};
        ByteBuffer testBuf = ByteBuffer.wrap(testBytes);
        testBuf.mark();
        long[] minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_INT8);
        Assert.assertEquals(minMax, new long[] {-128, 127});
        testBuf.reset();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_UINT8);
        Assert.assertEquals(minMax, new long[] {0, 255});

        short[] testShorts = new short[] {-32768, 0, 32767, -1};
        testBuf = ByteBuffer.allocate(testShorts.length*2);
        testBuf.asShortBuffer().put(testShorts);
        testBuf.mark();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_INT16);
        Assert.assertEquals(minMax, new long[] {-32768, 32767});
        testBuf.reset();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_UINT16);
        Assert.assertEquals(minMax, new long[] {0, 65535});

        int[] testInts = new int[] {-2147483648, 0, 2147483647, -1};
        testBuf = ByteBuffer.allocate(testInts.length*4);
        testBuf.asIntBuffer().put(testInts);
        testBuf.mark();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_INT32);
        Assert.assertEquals(minMax, new long[] {-2147483648, 2147483647});
        testBuf.reset();
        minMax = TiledbUtils.getMinMax(testBuf, Datatype.TILEDB_UINT32);
        Assert.assertEquals(minMax, new long[] {0, 4294967295L});
    }

}
