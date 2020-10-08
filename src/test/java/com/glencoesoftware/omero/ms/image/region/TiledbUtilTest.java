package com.glencoesoftware.omero.ms.image.region;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TiledbUtilTest {

    @Test public void getSubarrayDomainStdTest() {
        long[] subArrayDomain = TiledbUtils.getSubarrayDomainFromString("[0,0,0,100:150,200:250]");
        Assert.assertTrue(Arrays.equals(subArrayDomain, new long[] {0,0,0,0,0,0,100,149,200,249}));
    }

}
