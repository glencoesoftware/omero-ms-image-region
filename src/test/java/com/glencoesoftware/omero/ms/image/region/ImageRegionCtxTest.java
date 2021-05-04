/*
 * Copyright (C) 2017 Glencoe Software, Inc. All rights reserved.
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

import java.io.IOException;
import java.util.Map;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import io.vertx.core.MultiMap;
import io.vertx.core.json.Json;
import ome.xml.model.primitives.Color;
import omero.constants.projection.ProjectionType;

public class ImageRegionCtxTest {

    final private long imageId = 123;
    final private int z = 1;
    final private int t = 1;
    final private float q = 0.8f;
    // tile
    final private int resolution = 0;
    final private int tileX = 0;
    final private int tileY = 1;
    final private String tile = String.format(
            "%d,%d,%d,1024,2048", resolution, tileX, tileY);
    // region
    final private int regionX = 1;
    final private int regionY = 2;
    final private int regionWidth = 3;
    final private int regionHeight = 4;
    final private String region = String.format(
            "%d,%d,%d,%d", regionX, regionY, regionWidth, regionHeight);
    // Channel info
    final private int channel0 = -1;
    final private int channel1 = 2;
    final private int channel2 = -3;
    final private double[] window0 = new double[]{0, 65535};
    final private double[] window1 = new double[]{1755, 51199};
    final private double[] window2 = new double[]{3218, 26623};
    final private String color0 = "0000FF";
    final private String color1 = "00FF00";
    final private String color2 = "FF0000";
    final private String c = String.format(
            "%d|%f:%f$%s,%d|%f:%f$%s,%d|%f:%f$%s",
            channel0, window0[0], window0[1], color0,
            channel1, window1[0], window1[1], color1,
            channel2, window2[0], window2[1], color2);
    final private String maps = "[{\"reverse\": {\"enabled\": false}}, " +
            "{\"reverse\": {\"enabled\": false}}, " +
            "{\"reverse\": {\"enabled\": false}}]";

    private MultiMap params;

    @BeforeMethod
    public void setUp() throws IOException {
        params = MultiMap.caseInsensitiveMultiMap();
        params.add("imageId", String.valueOf(imageId));
        params.add("theZ", String.valueOf(z));
        params.add("theT", String.valueOf(t));
        params.add("q", String.valueOf(q));

        params.add("tile", tile);
        params.add("c", c);

        params.add("region", region);
        params.add("c", c);
        params.add("maps", maps);
    }

    private void assertChannelInfo(ImageRegionCtx imageCtx) {
        Assert.assertEquals(imageCtx.compressionQuality, q);

        Assert.assertEquals(imageCtx.colors.size(), 3);
        Assert.assertEquals(imageCtx.windows.size(), 3);
        Assert.assertEquals(imageCtx.channels.size(), 3);
        Assert.assertEquals(imageCtx.colors.get(0), color0);
        Assert.assertEquals(imageCtx.colors.get(1), color1);
        Assert.assertEquals(imageCtx.colors.get(2), color2);

        Assert.assertEquals((int) imageCtx.channels.get(0), channel0);
        Assert.assertEquals((int) imageCtx.channels.get(1), channel1);
        Assert.assertEquals((int) imageCtx.channels.get(2), channel2);

        Assert.assertEquals(imageCtx.windows.get(0)[0], window0[0]);
        Assert.assertEquals(imageCtx.windows.get(0)[1], window0[1]);
        Assert.assertEquals(imageCtx.windows.get(1)[0], window1[0]);
        Assert.assertEquals(imageCtx.windows.get(1)[1], window1[1]);
        Assert.assertEquals(imageCtx.windows.get(2)[0], window2[0]);
        Assert.assertEquals(imageCtx.windows.get(2)[1], window2[1]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingImageId()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("imageId");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testImageIdFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("imageId", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingTheZ()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("theZ");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTheZFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("theZ", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMissingTheT()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("theT");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTheTFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("theT", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRegionFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("region", "1,2,3,abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testChannelFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("c", "-1|0:65535$0000FF,a|1755:51199$00FF00,3|3218:26623$FF0000");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testChannelFormatActive()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("c", "-1|0:65535$0000FF,a|1755:51199$00FF00,3|3218:26623$FF0000");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testChannelFormatRange()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("c", "-1|0:65535$0000FF,1|abc:51199$00FF00,3|3218:26623$FF0000");
        new ImageRegionCtx(params, "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testQualityFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("q", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test
    public void testTileShortParameters()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("region");
        params.set("tile", String.format("%d,%d,%d", resolution, tileX, tileY));

        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);

        Assert.assertNull(imageCtxDecoded.region);
        Assert.assertNotNull(imageCtxDecoded.tile);
        Assert.assertEquals(imageCtxDecoded.tile.getX(), tileX);
        Assert.assertEquals(imageCtxDecoded.tile.getY(), tileY);
        Assert.assertEquals(imageCtxDecoded.tile.getWidth(), 0);
        Assert.assertEquals(imageCtxDecoded.tile.getHeight(), 0);
        Assert.assertEquals((int) imageCtxDecoded.resolution, resolution);
        assertChannelInfo(imageCtxDecoded);
    }

    @Test
    public void testTileParameters()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("region");
        params.set("m", "c");

        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);

        Assert.assertEquals(imageCtxDecoded.m, "rgb");
        Assert.assertNotNull(imageCtxDecoded.tile);
        Assert.assertEquals(imageCtxDecoded.tile.getX(), tileX);
        Assert.assertEquals(imageCtxDecoded.tile.getY(), tileY);
        Assert.assertEquals(imageCtxDecoded.tile.getWidth(), 1024);
        Assert.assertEquals(imageCtxDecoded.tile.getHeight(), 2048);
        Assert.assertEquals((int) imageCtxDecoded.resolution, resolution);
        assertChannelInfo(imageCtxDecoded);
    }

    @Test
    public void testRegionParameters()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("tile");
        params.set("m", "g");

        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertNull(imageCtxDecoded.tile);
        Assert.assertNull(imageCtxDecoded.resolution);
        Assert.assertEquals(imageCtxDecoded.m, "greyscale");
        Assert.assertNotNull(imageCtxDecoded.region);
        Assert.assertEquals(imageCtxDecoded.region.getX(), regionX);
        Assert.assertEquals(imageCtxDecoded.region.getY(), regionY);
        Assert.assertEquals(imageCtxDecoded.region.getWidth(), regionWidth);
        Assert.assertEquals(imageCtxDecoded.region.getHeight(), regionHeight);
        assertChannelInfo(imageCtxDecoded);
    }

    @Test
    public void testCodomainMaps()
            throws JsonParseException, JsonMappingException, IOException {
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertNotNull(imageCtxDecoded.maps);
        Assert.assertEquals(3, imageCtxDecoded.maps.size());
        for (Map<String, Map<String, Object>> map : imageCtxDecoded.maps) {
            Map<String, Object> reverse = map.get("reverse");
            Boolean enabled = (Boolean) reverse.get("enabled");
            Assert.assertFalse(enabled);
        }
    }

    @Test
    public void testProjectionIntMax()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("p", "intmax");
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertEquals(
                imageCtxDecoded.projection, ProjectionType.MAXIMUMINTENSITY);
        Assert.assertNull(imageCtxDecoded.projectionStart);
        Assert.assertNull(imageCtxDecoded.projectionEnd);
    }

    @Test
    public void testProjectionIntMean()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("p", "intmean");
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertEquals(
                imageCtxDecoded.projection, ProjectionType.MEANINTENSITY);
        Assert.assertNull(imageCtxDecoded.projectionStart);
        Assert.assertNull(imageCtxDecoded.projectionEnd);
    }

    @Test
    public void testProjectionIntSum()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("p", "intsum");
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertEquals(
                imageCtxDecoded.projection, ProjectionType.SUMINTENSITY);
        Assert.assertNull(imageCtxDecoded.projectionStart);
        Assert.assertNull(imageCtxDecoded.projectionEnd);
    }

    @Test
    public void testProjectionNormal()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("p", "normal");
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertNull(imageCtxDecoded.projection);
        Assert.assertNull(imageCtxDecoded.projectionStart);
        Assert.assertNull(imageCtxDecoded.projectionEnd);
    }

    @Test
    public void testProjectionIntMeanStartEnd()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("p", "intmax|0:1");
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertEquals(
                imageCtxDecoded.projection, ProjectionType.MAXIMUMINTENSITY);
        Assert.assertEquals(imageCtxDecoded.projectionStart, new Integer(0));
        Assert.assertEquals(imageCtxDecoded.projectionEnd, new Integer(1));
    }

    @Test
    public void testProjectionIntMeanStartEndInvalid()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("p", "intmax|a:b");
        ImageRegionCtx imageCtx = new ImageRegionCtx(params, "");
        String data = Json.encode(imageCtx);
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageCtxDecoded = mapper.readValue(
                data, ImageRegionCtx.class);
        Assert.assertEquals(
                imageCtxDecoded.projection, ProjectionType.MAXIMUMINTENSITY);
        Assert.assertNull(imageCtxDecoded.projectionStart);
        Assert.assertNull(imageCtxDecoded.projectionEnd);
    }

    @Test
    public void testRedOpaque() {
        Integer rgba = ImageRegionCtx.splitHTMLColor("FF0000FF");
        Color c = new Color(rgba);
        Assert.assertEquals(255, c.getRed());
        Assert.assertEquals(0, c.getGreen());
        Assert.assertEquals(0, c.getBlue());
        Assert.assertEquals(255, c.getAlpha());
    }

    @Test
    public void testRedTransparent() {
        Integer rgba = ImageRegionCtx.splitHTMLColor("FF000000");
        Color c = new Color(rgba);
        Assert.assertEquals(255, c.getRed());
        Assert.assertEquals(0, c.getGreen());
        Assert.assertEquals(0, c.getBlue());
        Assert.assertEquals(0, c.getAlpha());
    }

    @Test
    public void testGreenOpaque() {
        Integer rgba = ImageRegionCtx.splitHTMLColor("00FF00FF");
        Color c = new Color(rgba);
        Assert.assertEquals(0, c.getRed());
        Assert.assertEquals(255, c.getGreen());
        Assert.assertEquals(0, c.getBlue());
        Assert.assertEquals(255, c.getAlpha());
    }

    @Test
    public void testBlueOpaque() {
        Integer rgba = ImageRegionCtx.splitHTMLColor("0000FFFF");
        Color c = new Color(rgba);
        Assert.assertEquals(0, c.getRed());
        Assert.assertEquals(0, c.getGreen());
        Assert.assertEquals(255, c.getBlue());
        Assert.assertEquals(255, c.getAlpha());
    }
}
