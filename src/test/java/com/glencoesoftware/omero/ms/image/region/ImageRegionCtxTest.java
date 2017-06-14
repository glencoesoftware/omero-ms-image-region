package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;
import java.util.Map;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;

import io.vertx.core.MultiMap;

public class ImageRegionCtxTest {

    MultiMap paramsTile = MultiMap.caseInsensitiveMultiMap();
    MultiMap paramsRegion = MultiMap.caseInsensitiveMultiMap();

    @BeforeTest
    public void setUp() throws IOException {
        paramsTile.add("tile", "0,1,1,1024,1024");
        paramsTile.add(
                "c",
                "-1|0:65535$0000FF,-2|1755:51199$00FF00,-3|3218:26623$FF0000");
        paramsTile.add("z", "1");
        paramsTile.add("t", "0");
        paramsTile.add("imageId", "123");
        paramsTile.add("m", "c");
        paramsTile.add("q", "0.8");

        paramsRegion.add("region", "0,0,1024,1024");
        paramsRegion.add(
                "c",
                "-1|0:65535$0000FF,-2|1755:51199$00FF00,-3|3218:26623$FF0000");
        paramsRegion.add("z", "1");
        paramsRegion.add("t", "0");
        paramsRegion.add("imageId", "123");
        paramsRegion.add("m", "c");
        paramsRegion.add("q", "0.8");
    }

    
    @Test
    public void testTileParameters() {
        ImageRegionCtx imageCtx = new ImageRegionCtx(paramsTile);
        Map<String, Object> params = imageCtx.getImageRegionFormatted();
        Assert.assertEquals(params.size(), 12);
        
    }

    @Test
    public void testRegionParameters() {
        ImageRegionCtx imageCtx = new ImageRegionCtx(paramsRegion);
        Map<String, Object> params = imageCtx.getImageRegionFormatted();
        Assert.assertEquals(params.size(), 12);
    }

}
