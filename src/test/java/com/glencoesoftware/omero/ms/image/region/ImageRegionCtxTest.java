package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;

import io.vertx.core.MultiMap;
import io.vertx.core.json.Json;

public class ImageRegionCtxTest {

    MultiMap paramsTile = MultiMap.caseInsensitiveMultiMap();
    MultiMap paramsRegion = MultiMap.caseInsensitiveMultiMap();

    final private long imageId = 123;
    final private int z = 1;
    final private int t = 1;
    final private String m1 = "c";
    final private String m2 = "g";
    final private float q = 0.8f;
    // tile
    final private int resolution = 0;
    final private int tileX = 0;
    final private int tileY = 1;
    final private String tile = String.format(
            "%d,%d,%d,1024,1024", resolution, tileX, tileY);
    // region
    final private int regionX = 0;
    final private int regionY = 0;
    final private int regionWidth = 1024;
    final private int regionHeight = 1024;
    final private String region = String.format(
            "%d,%d,%d,%d", regionX, regionY, regionWidth, regionHeight);
    // Channel info
    final private int channel0 = -1;
    final private int channel1 = 2;
    final private int channel2 = -3;
    final private float[] window0 = new float[]{0, 65535};
    final private float[] window1 = new float[]{1755, 51199};
    final private float[] window2 = new float[]{3218, 26623};
    final private String color0 = "0000FF";
    final private String color1 = "00FF00";
    final private String color2 = "FF0000";
    final private String c = String.format(
            "%d|%f:%f$%s,%d|%f:%f$%s,%d|%f:%f$%s",
            channel0, window0[0], window0[1], color0,
            channel1, window1[0], window1[1], color1,
            channel2, window2[0], window2[1], color2);

    @BeforeTest
    public void setUp() throws IOException {
        paramsTile.add("tile", tile);
        paramsTile.add("c", c);
        paramsTile.add("z", String.valueOf(z));
        paramsTile.add("t", String.valueOf(t));
        paramsTile.add("imageId", String.valueOf(imageId));
        paramsTile.add("m", m1);
        paramsTile.add("q", String.valueOf(q));

        paramsRegion.add("region", region);
        paramsRegion.add("c", c);
        paramsRegion.add("z", String.valueOf(z));
        paramsRegion.add("t", String.valueOf(t));
        paramsRegion.add("imageId", String.valueOf(imageId));
        paramsRegion.add("m", m2);
    }

    private void checkChannelInfo(ImageRegionCtx imageCtx) {
        Assert.assertEquals(imageCtx.colors.size(), 3);
        Assert.assertEquals(imageCtx.windows.size(), 3);
        Assert.assertEquals(imageCtx.channels.size(), 3);
        Assert.assertEquals(imageCtx.colors.get(0), color0);
        Assert.assertEquals(imageCtx.colors.get(1), color1);
        Assert.assertEquals(imageCtx.colors.get(2), color2);

        Assert.assertEquals((int) imageCtx.channels.get(0), channel0);
        Assert.assertEquals((int) imageCtx.channels.get(1), channel1);
        Assert.assertEquals((int) imageCtx.channels.get(2), channel2);

        Assert.assertEquals((float) imageCtx.windows.get(0)[0], window0[0]);
        Assert.assertEquals((float) imageCtx.windows.get(0)[1], window0[1]);
        Assert.assertEquals((float) imageCtx.windows.get(1)[0], window1[0]);
        Assert.assertEquals((float) imageCtx.windows.get(1)[1], window1[1]);
        Assert.assertEquals((float) imageCtx.windows.get(2)[0], window2[0]);
        Assert.assertEquals((float) imageCtx.windows.get(2)[1], window2[1]);
    }

    @Test
    public void testTileParameters()
            throws JsonParseException, JsonMappingException, IOException
    {
        ImageRegionCtx imageCtx = new ImageRegionCtx(paramsTile);
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertEquals(imageCtxDecoded.m, "rgb");
        Assert.assertEquals(imageCtxDecoded.compressionQuality, q);
        Assert.assertEquals((int) imageCtxDecoded.resolution, resolution);
        Assert.assertEquals(imageCtxDecoded.tile.size(), 2);
        Assert.assertEquals((int) imageCtxDecoded.tile.get(0), tileX);
        Assert.assertEquals((int) imageCtxDecoded.tile.get(1), tileY);
        checkChannelInfo(imageCtxDecoded);
    }

    @Test
    public void testRegionParameters()
            throws JsonParseException, JsonMappingException, IOException
    {
        ImageRegionCtx imageCtx = new ImageRegionCtx(paramsRegion);
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertNull(imageCtxDecoded.tile);
        Assert.assertNull(imageCtxDecoded.resolution);
        Assert.assertNull(imageCtxDecoded.compressionQuality);
        Assert.assertEquals(imageCtxDecoded.m, "greyscale");
        Assert.assertEquals(imageCtxDecoded.region.size(), 4);
        Assert.assertEquals((int) imageCtxDecoded.region.get(0), regionX);
        Assert.assertEquals((int) imageCtxDecoded.region.get(1), regionY);
        Assert.assertEquals((int) imageCtxDecoded.region.get(2), regionWidth);
        Assert.assertEquals((int) imageCtxDecoded.region.get(3), regionHeight);
        checkChannelInfo(imageCtxDecoded);
    }

}
