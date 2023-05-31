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
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 4);
        Assert.assertEquals(testData.getValue(0), 0);
        Assert.assertEquals(testData.getValue(1), 1);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 2);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testValGreaterThanMax() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {1,2,3,4});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 3};
        histogramCtx.bins = 4;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 4);
        Assert.assertEquals(testData.getValue(0), 0);
        Assert.assertEquals(testData.getValue(1), 1);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 1);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 1);
        }

    @Test
    public void testValLessThanMin() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {1,2,3,4});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {2, 4};
        histogramCtx.bins = 4;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 4);
        Assert.assertEquals(testData.getValue(0), 1);
        Assert.assertEquals(testData.getValue(1), 0);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 1);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 1);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testMoreBinsThanValues() {
        ByteBuffer bb = ByteBuffer.allocate(3);
        bb.put(new byte[] {0,1,3});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 3};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(5, testData.size());
        Assert.assertEquals(1, (int) testData.getInteger(0));
        Assert.assertEquals(1, (int) testData.getInteger(1));
        Assert.assertEquals(0, (int) testData.getInteger(2));
        Assert.assertEquals(0, (int) testData.getInteger(3));
        Assert.assertEquals(1, (int) testData.getInteger(4));
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testRangeLargerThanData() {
        ByteBuffer bb = ByteBuffer.allocate(3);
        bb.put(new byte[] {3,4,5});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 9};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(5, testData.size());
        Assert.assertEquals(0, (int) testData.getInteger(0));
        Assert.assertEquals(1, (int) testData.getInteger(1)); //3
        Assert.assertEquals(2, (int) testData.getInteger(2)); //4, 5
        Assert.assertEquals(0, (int) testData.getInteger(3));
        Assert.assertEquals(0, (int) testData.getInteger(4));
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testPixelDataLargerThanMaxPlaneSize() {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(new byte[] {0,1,2,3});
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 3};
        histogramCtx.bins = 2;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(2, testData.size());
        Assert.assertEquals(2, (int) testData.getInteger(0)); //0,1
        Assert.assertEquals(2, (int) testData.getInteger(1)); //2.3
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testUINT8() {
        ByteBuffer bb = ByteBuffer.allocate(10);
        bb.put((byte) (175 & 0xff));
        bb.put((byte) (153 & 0xff));
        bb.put((byte) (63 & 0xff));
        bb.put((byte) (173 & 0xff));
        bb.put((byte) (143 & 0xff));
        bb.put((byte) (218 & 0xff));
        bb.put((byte) (145 & 0xff));
        bb.put((byte) (39 & 0xff));
        bb.put((byte) (138 & 0xff));
        bb.put((byte) (3 & 0xff));
        PixelData pd = new PixelData(PixelsType.VALUE_UINT8, bb);
        double[] minMax = new double[] {0, 255};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 2);
        Assert.assertEquals(testData.getValue(1), 1);
        Assert.assertEquals(testData.getValue(2), 3);
        Assert.assertEquals(testData.getValue(3), 3);
        Assert.assertEquals(testData.getValue(4), 1);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testINT8() {
        ByteBuffer bb = ByteBuffer.allocate(10);
        bb.put((byte) 101);
        bb.put((byte) -50);
        bb.put((byte) 82);
        bb.put((byte) 40);
        bb.put((byte) -68);
        bb.put((byte) -18);
        bb.put((byte) -92);
        bb.put((byte) -95);
        bb.put((byte) 48);
        bb.put((byte) -78);
        PixelData pd = new PixelData(PixelsType.VALUE_INT8, bb);
        double[] minMax = new double[] {-128, 127};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 3);
        Assert.assertEquals(testData.getValue(1), 2);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 2);
        Assert.assertEquals(testData.getValue(4), 2);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testUINT16() {
        ByteBuffer bb = ByteBuffer.allocate(20);
        bb.putShort((short) (21533 & 0xffff));
        bb.putShort((short) (38752 & 0xffff));
        bb.putShort((short) (36269 & 0xffff));
        bb.putShort((short) (2760 & 0xffff));
        bb.putShort((short) (1425 & 0xffff));
        bb.putShort((short) (14880 & 0xffff));
        bb.putShort((short) (34827 & 0xffff));
        bb.putShort((short) (11986 & 0xffff));
        bb.putShort((short) (11077 & 0xffff));
        bb.putShort((short) (46156 & 0xffff));
        PixelData pd = new PixelData(PixelsType.VALUE_UINT16, bb);
        double[] minMax = new double[] {0, 65535};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 4);
        Assert.assertEquals(testData.getValue(1), 2);
        Assert.assertEquals(testData.getValue(2), 3);
        Assert.assertEquals(testData.getValue(3), 1);
        Assert.assertEquals(testData.getValue(4), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testINT16() {
        ByteBuffer bb = ByteBuffer.allocate(20);
        bb.putShort((short) -19497);
        bb.putShort((short) -11285);
        bb.putShort((short) 1187);
        bb.putShort((short) -17219);
        bb.putShort((short) 26833);
        bb.putShort((short) 28521);
        bb.putShort((short) -31711);
        bb.putShort((short) -21571);
        bb.putShort((short) -12075);
        bb.putShort((short) 6841);
        PixelData pd = new PixelData(PixelsType.VALUE_INT16, bb);
        double[] minMax = new double[] {-32768, 32767};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 2);
        Assert.assertEquals(testData.getValue(1), 4);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 1);
        Assert.assertEquals(testData.getValue(4), 2);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testUINT32() {
        ByteBuffer bb = ByteBuffer.allocate(40);
        bb.putInt((int) (1039053181L & 0xffffffffL));
        bb.putInt((int) (1966804319L & 0xffffffffL));
        bb.putInt((int) (3513814796L & 0xffffffffL));
        bb.putInt((int) (642532262L & 0xffffffffL));
        bb.putInt((int) (3411013609L & 0xffffffffL));
        bb.putInt((int) (433606742L & 0xffffffffL));
        bb.putInt((int) (2289077882L & 0xffffffffL));
        bb.putInt((int) (4100655705L & 0xffffffffL));
        bb.putInt((int) (3585211196L & 0xffffffffL));
        bb.putInt((int) (1356288417L & 0xffffffffL));
        PixelData pd = new PixelData(PixelsType.VALUE_UINT32, bb);
        double[] minMax = new double[] {0, 4294967295L};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 2);
        Assert.assertEquals(testData.getValue(1), 2);
        Assert.assertEquals(testData.getValue(2), 2);
        Assert.assertEquals(testData.getValue(3), 1);
        Assert.assertEquals(testData.getValue(4), 3);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testINT32() {
        ByteBuffer bb = ByteBuffer.allocate(40);
        bb.putInt((int) 314025421);
        bb.putInt((int) -2068008231);
        bb.putInt((int) 122426751);
        bb.putInt((int) 308854092);
        bb.putInt((int) 1524117449);
        bb.putInt((int) -936228904);
        bb.putInt((int) -1400983178);
        bb.putInt((int) 131007035);
        bb.putInt((int) -1762565678);
        bb.putInt((int) -841111877);
        PixelData pd = new PixelData(PixelsType.VALUE_INT32, bb);
        double[] minMax = new double[] {-2147483648, 2147483647};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 3);
        Assert.assertEquals(testData.getValue(1), 2);
        Assert.assertEquals(testData.getValue(2), 4);
        Assert.assertEquals(testData.getValue(3), 0);
        Assert.assertEquals(testData.getValue(4), 1);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testFLOAT() {
        ByteBuffer bb = ByteBuffer.allocate(40);
        bb.putFloat((float) 0.3194745);
        bb.putFloat((float) 0.4041198);
        bb.putFloat((float) 0.5212703);
        bb.putFloat((float) 0.8714385);
        bb.putFloat((float) 0.58057505);
        bb.putFloat((float) 0.7913712);
        bb.putFloat((float) 0.6242399);
        bb.putFloat((float) 0.0665013);
        bb.putFloat((float) 0.25559694);
        bb.putFloat((float) 0.7946202);
        PixelData pd = new PixelData(PixelsType.VALUE_FLOAT, bb);
        double[] minMax = new double[] {-1, 1};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 0);
        Assert.assertEquals(testData.getValue(1), 0);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 5);
        Assert.assertEquals(testData.getValue(4), 4);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }

    @Test
    public void testDOUBLE() {
        ByteBuffer bb = ByteBuffer.allocate(80);
        bb.putDouble(0.3194745);
        bb.putDouble(0.4041198);
        bb.putDouble(0.5212703);
        bb.putDouble(0.8714385);
        bb.putDouble(0.58057505);
        bb.putDouble(0.7913712);
        bb.putDouble(0.6242399);
        bb.putDouble(0.0665013);
        bb.putDouble(0.25559694);
        bb.putDouble(0.7946202);
        PixelData pd = new PixelData(PixelsType.VALUE_DOUBLE, bb);
        double[] minMax = new double[] {-1, 1};
        histogramCtx.bins = 5;
        JsonObject histogramData = requestHandler.getHistogramData(pd, minMax);
        JsonArray testData = histogramData.getJsonArray(
            HistogramRequestHandler.HISTOGRAM_DATA_KEY);
        Assert.assertEquals(testData.size(), 5);
        Assert.assertEquals(testData.getValue(0), 0);
        Assert.assertEquals(testData.getValue(1), 0);
        Assert.assertEquals(testData.getValue(2), 1);
        Assert.assertEquals(testData.getValue(3), 5);
        Assert.assertEquals(testData.getValue(4), 4);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.LEFT_OUTLIER_COUNT_KEY).intValue(), 0);
        Assert.assertEquals(histogramData.getInteger(
                HistogramRequestHandler.RIGHT_OUTLIER_COUNT_KEY).intValue(), 0);
    }
}
