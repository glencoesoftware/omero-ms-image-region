package com.glencoesoftware.omero.ms.image.region;


import org.junit.Test;

import io.vertx.core.MultiMap;

import org.junit.Assert;
import org.junit.Before;

public class HistogramCtxTest {

    private MultiMap params;

    @Before
    public void setUp() {
        params = MultiMap.caseInsensitiveMultiMap();
        params.add("imageId", "123");
        params.add("theC", "1");
        params.add("z", "2");
        params.add("t", "3");
        params.add("bins", "10");
        params.add("maxPlaneWidth", "3000");
        params.add("maxPlaneHeight", "4000");
        params.add("usePixelsTypeRange", "false");
    }

    @Test
    public void testHistogramCorrect() {
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(Long.valueOf(123), ctx.imageId);
        Assert.assertEquals(Integer.valueOf(1), ctx.c);
        Assert.assertEquals(Integer.valueOf(2), ctx.z);
        Assert.assertEquals(Integer.valueOf(3), ctx.t);
        Assert.assertEquals(Integer.valueOf(10), ctx.bins);
        Assert.assertEquals(Integer.valueOf(3000), ctx.maxPlaneWidth);
        Assert.assertEquals(Integer.valueOf(4000), ctx.maxPlaneHeight);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingImageId() {
        params.remove("imageId");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingC() {
        params.remove("theC");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingZ() {
        params.remove("z");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingT() {
        params.remove("t");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
    }

    @Test
    public void testMissingBins() {
        params.remove("bins");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(Integer.valueOf(256), ctx.bins);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMaxPlaneWidth() {
        params.remove("maxPlaneWidth");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingMaxPlaneHeight() {
        params.remove("maxPlaneHeight");
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
    }

    @Test
    public void testUsePixelsTypeRange() {
        //Check value is "false"
        HistogramCtx ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(false, ctx.usePixelsTypeRange);
        //Check value us missing
        params.remove("usePixelsTypeRange");
        ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(false, ctx.usePixelsTypeRange);
        //Check all non-true strings are false
        params.add("usePixelsTypeRange", "somestring");
        ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(false, ctx.usePixelsTypeRange);
        //Check lower case true
        params.remove("usePixelsTypeRange");
        params.add("usePixelsTypeRange", "true");
        ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(true, ctx.usePixelsTypeRange);
        //Check upper case true
        params.remove("usePixelsTypeRange");
        params.add("usePixelsTypeRange", "TRUE");
        ctx = new HistogramCtx(params, "abc123");
        Assert.assertEquals(true, ctx.usePixelsTypeRange);
    }
}
