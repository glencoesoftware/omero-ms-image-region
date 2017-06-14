package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;

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
    final private int[] window0 = new int[]{0, 65535};
    final private int[] window1 = new int[]{1755, 51199};
    final private int[] window2 = new int[]{3218, 26623};
    final private String color0 = "0000FF";
    final private String color1 = "00FF00";
    final private String color2 = "FF0000";
    final private String c = String.format(
            "%d|%d:%d$%s,%d|%d:%d$%s,%d|%d:%d$%s",
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

    private void checkChannelInfo(HashMap<String, Object> channelInfo) {
        ArrayList<Integer> channels =
                (ArrayList<Integer>) channelInfo.get("active");
        ArrayList<Integer[] > windows =
                (ArrayList<Integer []>) channelInfo.get("windows");
        ArrayList<String> colors =
                (ArrayList<String>) channelInfo.get("colors");

        Assert.assertEquals(colors.size(), 3);
        Assert.assertEquals(windows.size(), 3);
        Assert.assertEquals(channels.size(), 3);
        Assert.assertEquals(colors.get(0), color0);
        Assert.assertEquals(colors.get(1), color1);
        Assert.assertEquals(colors.get(2), color2);

        Assert.assertEquals((int) channels.get(0), channel0);
        Assert.assertEquals((int) channels.get(1), channel1);
        Assert.assertEquals((int) channels.get(2), channel2);

        Assert.assertEquals((int) windows.get(0)[0], window0[0]);
        Assert.assertEquals((int) windows.get(0)[1], window0[1]);
        Assert.assertEquals((int) windows.get(1)[0], window1[0]);
        Assert.assertEquals((int) windows.get(1)[1], window1[1]);
        Assert.assertEquals((int) windows.get(2)[0], window2[0]);
        Assert.assertEquals((int) windows.get(2)[1], window2[1]);
    }
    
    @Test
    public void testTileParameters() {
        ImageRegionCtx imageCtx = new ImageRegionCtx(paramsTile);
        Map<String, Object> params = imageCtx.getImageRegionFormatted();
        Assert.assertEquals(params.size(), 12);
        Assert.assertNull(params.get("region"));
        Assert.assertEquals(params.get("m"), "rgb");
        Assert.assertEquals(params.get("compressionQuality"), q);
        Assert.assertEquals(params.get("resolution"), resolution);
        JsonArray tileArray = (JsonArray) params.get("tile");
        Assert.assertEquals(tileArray.size(), 2);
        Assert.assertEquals((int) tileArray.getInteger(0), tileX);
        Assert.assertEquals((int) tileArray.getInteger(1), tileY);
        // Check channel info
        HashMap<String, Object> channelInfo =
                (HashMap<String, Object>) params.get("channelInfo");
        this.checkChannelInfo(channelInfo);
    }

    @Test
    public void testRegionParameters() {
        ImageRegionCtx imageCtx = new ImageRegionCtx(paramsRegion);
        Map<String, Object> params = imageCtx.getImageRegionFormatted();
        Assert.assertEquals(params.size(), 12);
        Assert.assertNull(params.get("tile"));
        Assert.assertNull(params.get("resolution"));
        Assert.assertNull(params.get("compressionQuality"));
        Assert.assertEquals(params.get("m"), "greyscale");
        JsonArray region = (JsonArray) params.get("region");
        Assert.assertEquals(region.size(), 4);
        Assert.assertEquals((int) region.getInteger(0), regionX);
        Assert.assertEquals((int) region.getInteger(1), regionY);
        Assert.assertEquals((int) region.getInteger(2), regionWidth);
        Assert.assertEquals((int) region.getInteger(3), regionHeight);
        HashMap<String, Object> channelInfo =
                (HashMap<String, Object>) params.get("channelInfo");
        this.checkChannelInfo(channelInfo);
    }

}
