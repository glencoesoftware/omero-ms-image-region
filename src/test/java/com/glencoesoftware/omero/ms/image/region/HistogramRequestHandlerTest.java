package com.glencoesoftware.omero.ms.image.region;


import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.junit.Assert;
import ome.model.enums.PixelsType;
import ome.util.PixelData;

public class HistogramRequestHandlerTest {

    private HistogramCtx histogramCtx;

    private HistogramRequestHandler requestHandler;

    @Before
    public void setUp() {
        MultiMap params = new CaseInsensitiveHeaders();

        params.add("imageId", "1");
        params.add("theC", "0");
        params.add("z", "0");
        params.add("t", "0");
        params.add("maxPlaneWidth", "2048");
        params.add("maxPlaneHeight", "2048");

        histogramCtx = new HistogramCtx(params, "");
        requestHandler = new HistogramRequestHandler(histogramCtx, null);
    }

    @Test
    public void testHistogramData() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {1,2,3,4});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 4};
        histogramCtx.bins = 4;
        JsonArray testData = requestHandler.getHistogramData(pd,
                minMax).getJsonArray(HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 4);
        Assert.assertEquals(testData.getValue(0), 1);
        Assert.assertEquals(testData.getValue(1), 1);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 1);
    }

    @Test
    public void testValGreaterThanMax() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {1,2,3,4});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 3};
        histogramCtx.bins = 4;
        JsonObject testData = requestHandler.getHistogramData(pd,
                minMax);
        Assert.assertEquals(testData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(testData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 1);
        }

    @Test
    public void testValLessThanMin() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {1,2,3,4});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {2, 4};
        histogramCtx.bins = 4;
        JsonObject testData = requestHandler.getHistogramData(pd,
                minMax);
        Assert.assertEquals(testData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 1);
        Assert.assertEquals(testData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testMoreBinsThanValues() {
        ByteBuffer bb = ByteBuffer.allocate(3);
        bb.put(new byte[] {0,1,3});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 3};
        histogramCtx.bins = 5;
        JsonArray testData = requestHandler.getHistogramData(pd,
                minMax).getJsonArray(HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(5, testData.size());
        Assert.assertEquals(1, (int) testData.getInteger(0));
        Assert.assertEquals(0, (int) testData.getInteger(1));
        Assert.assertEquals(1, (int) testData.getInteger(2));
        Assert.assertEquals(0, (int) testData.getInteger(3));
        Assert.assertEquals(1, (int) testData.getInteger(4));
    }

    @Test
    public void testRangeLargerThanData() {
        ByteBuffer bb = ByteBuffer.allocate(3);
        bb.put(new byte[] {3,4,5});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 9};
        histogramCtx.bins = 5;
        JsonArray testData = requestHandler.getHistogramData(pd,
                minMax).getJsonArray(HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(5, testData.size());
        Assert.assertEquals(0, (int) testData.getInteger(0));
        Assert.assertEquals(1, (int) testData.getInteger(1)); //3
        Assert.assertEquals(2, (int) testData.getInteger(2)); //4, 5
        Assert.assertEquals(0, (int) testData.getInteger(3));
        Assert.assertEquals(0, (int) testData.getInteger(4));
    }

    @Test
    public void testPixelDataLargerThanMaxPlaneSize() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {0,1,2,3});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 3};
        histogramCtx.bins = 2;
        JsonArray testData = requestHandler.getHistogramData(pd,
                minMax).getJsonArray(HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(2, testData.size());
        Assert.assertEquals(2, (int) testData.getInteger(0)); //0,1
        Assert.assertEquals(2, (int) testData.getInteger(1)); //2.3
    }

}
