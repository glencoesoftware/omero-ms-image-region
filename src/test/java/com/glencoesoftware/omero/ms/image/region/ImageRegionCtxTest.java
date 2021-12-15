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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.awt.Dimension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

import io.vertx.core.MultiMap;
import io.vertx.core.json.Json;
import ome.io.nio.PixelBuffer;
import ome.model.core.Channel;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.QuantumDef;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.PixelsType;
import ome.model.enums.RenderingModel;
import ome.xml.model.primitives.Color;
import omeis.providers.re.Renderer;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.quantum.QuantumFactory;
import omero.ServerError;
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

    @Before
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
        Assert.assertEquals(imageCtx.compressionQuality, q, 0);

        Assert.assertEquals(imageCtx.colors.size(), 3);
        Assert.assertEquals(imageCtx.windows.size(), 3);
        Assert.assertEquals(imageCtx.channels.size(), 3);
        Assert.assertEquals(imageCtx.colors.get(0), color0);
        Assert.assertEquals(imageCtx.colors.get(1), color1);
        Assert.assertEquals(imageCtx.colors.get(2), color2);

        Assert.assertEquals((int) imageCtx.channels.get(0), channel0);
        Assert.assertEquals((int) imageCtx.channels.get(1), channel1);
        Assert.assertEquals((int) imageCtx.channels.get(2), channel2);

        Assert.assertEquals(imageCtx.windows.get(0)[0], window0[0], 0);
        Assert.assertEquals(imageCtx.windows.get(0)[1], window0[1], 0);
        Assert.assertEquals(imageCtx.windows.get(1)[0], window1[0], 0);
        Assert.assertEquals(imageCtx.windows.get(1)[1], window1[1], 0);
        Assert.assertEquals(imageCtx.windows.get(2)[0], window2[0], 0);
        Assert.assertEquals(imageCtx.windows.get(2)[1], window2[1], 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingImageId()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("imageId");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testImageIdFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("imageId", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingTheZ()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("theZ");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTheZFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("theZ", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingTheT()
            throws JsonParseException, JsonMappingException, IOException {
        params.remove("theT");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTheTFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("theT", "abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegionFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("region", "1,2,3,abc");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testChannelFormat()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("c", "-1|0:65535$0000FF,a|1755:51199$00FF00,3|3218:26623$FF0000");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testChannelFormatActive()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("c", "-1|0:65535$0000FF,a|1755:51199$00FF00,3|3218:26623$FF0000");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testChannelFormatRange()
            throws JsonParseException, JsonMappingException, IOException {
        params.set("c", "-1|0:65535$0000FF,1|abc:51199$00FF00,3|3218:26623$FF0000");
        new ImageRegionCtx(params, "");
    }

    @Test(expected = IllegalArgumentException.class)
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

    public Renderer getRenderer() {
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        //families.add(new Family(Family.VALUE_POLYNOMIAL));
        QuantumFactory quantumFactory = new QuantumFactory(families);
        List<RenderingModel> renderingModels = new ArrayList<RenderingModel>();
        renderingModels.add(new RenderingModel(RenderingModel.VALUE_RGB));
        Pixels pixels = new Pixels();
        Channel c1 = new Channel();
        c1.setRed(0);
        c1.setGreen(0);
        c1.setBlue(0);
        c1.setAlpha(0);
        Channel c2 = new Channel();
        c2.setRed(0);
        c2.setGreen(0);
        c2.setBlue(0);
        c2.setAlpha(0);
        Channel c3 = new Channel();
        c3.setRed(0);
        c3.setGreen(0);
        c3.setBlue(0);
        c3.setAlpha(0);
        pixels.addChannel(c1);
        pixels.addChannel(c2);
        pixels.addChannel(c3);
        pixels.setSizeC(3);
        PixelsType type = new PixelsType(PixelsType.VALUE_INT32);
        type.setBitSize(8);
        pixels.setPixelsType(type);
        pixels.setSignificantBits(8);
        RenderingDef rdef = new RenderingDef();
        QuantumDef qdef = new QuantumDef();
        qdef.setBitResolution(QuantumFactory.DEPTH_8BIT);
        qdef.setCdStart(0);
        qdef.setCdEnd(1);
        rdef.setQuantization(qdef);
        rdef.setModel(new RenderingModel(RenderingModel.VALUE_RGB));
        ChannelBinding cb1 = new ChannelBinding();
        cb1.setFamily(new Family(Family.VALUE_LINEAR));
        cb1.setCoefficient(1.0);
        cb1.setNoiseReduction(false);
        cb1.setInputStart(0.0);
        cb1.setInputEnd(1.0);
        cb1.setActive(true);
        cb1.setRed(0);
        cb1.setGreen(0);
        cb1.setBlue(0);
        cb1.setAlpha(0);
        ChannelBinding cb2 = new ChannelBinding();
        cb2.setFamily(new Family(Family.VALUE_LINEAR));
        cb2.setCoefficient(1.0);
        cb2.setNoiseReduction(false);
        cb2.setInputStart(0.0);
        cb2.setInputEnd(1.0);
        cb2.setActive(true);
        cb2.setRed(0);
        cb2.setGreen(0);
        cb2.setBlue(0);
        cb2.setAlpha(0);
        ChannelBinding cb3 = new ChannelBinding();
        cb3.setFamily(new Family(Family.VALUE_LINEAR));
        cb3.setCoefficient(1.0);
        cb3.setNoiseReduction(false);
        cb3.setInputStart(0.0);
        cb3.setInputEnd(1.0);
        cb3.setRed(0);
        cb3.setGreen(0);
        cb3.setBlue(0);
        cb3.setAlpha(0);
        rdef.addChannelBinding(cb1);
        rdef.addChannelBinding(cb2);
        rdef.addChannelBinding(cb3);
        cb3.setActive(true);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getResolutionLevel()).thenReturn(3);
        Renderer renderer = new Renderer(quantumFactory, renderingModels, pixels, rdef, pixelBuffer, null);
        return renderer;
    }

    @Test
    public void testRendering() {
        Renderer renderer = getRenderer();
        Color c1 = ImageRegionCtx.splitHTMLColor("FF0000FF");
        renderer.setRGBA(0, c1.getRed(), c1.getGreen(), c1.getBlue(), c1.getAlpha());
        Color c2 = ImageRegionCtx.splitHTMLColor("00FF00FF");
        renderer.setRGBA(1, c2.getRed(), c2.getGreen(), c2.getBlue(), c2.getAlpha());
        Color c3 = ImageRegionCtx.splitHTMLColor("0000FFFF");
        renderer.setRGBA(2, c3.getRed(), c3.getGreen(), c3.getBlue(), c3.getAlpha());

        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getRed(), new Integer(255));
        Assert.assertEquals(cb.getGreen(), new Integer(0));
        Assert.assertEquals(cb.getBlue(), new Integer(0));
        Assert.assertEquals(cb.getAlpha(), new Integer(255));

        cb = renderer.getChannelBindings()[1];
        Assert.assertEquals(cb.getRed(), new Integer(0));
        Assert.assertEquals(cb.getGreen(), new Integer(255));
        Assert.assertEquals(cb.getBlue(), new Integer(0));
        Assert.assertEquals(cb.getAlpha(), new Integer(255));

        cb = renderer.getChannelBindings()[2];
        Assert.assertEquals(cb.getRed(), new Integer(0));
        Assert.assertEquals(cb.getGreen(), new Integer(0));
        Assert.assertEquals(cb.getBlue(), new Integer(255));
        Assert.assertEquals(cb.getAlpha(), new Integer(255));

    }

    @Test
    public void testWindow() {
        Renderer renderer = getRenderer();
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        for(int i = 0; i < ctx.windows.size(); i++) {
            ctx.setWindow(renderer, i, i);
        }
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getInputStart(), window0[0], 0);
        Assert.assertEquals(cb.getInputEnd(), window0[1], 0);

        cb = renderer.getChannelBindings()[1];
        Assert.assertEquals(cb.getInputStart(), window1[0], 0);
        Assert.assertEquals(cb.getInputEnd(), window1[1], 0);

        cb = renderer.getChannelBindings()[2];
        Assert.assertEquals(cb.getInputStart(), window2[0], 0);
        Assert.assertEquals(cb.getInputEnd(), window2[1], 0);
    }

    @Test
    public void testMapsLinear() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"linear\",\"coefficient\":1.8}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "linear");
        Assert.assertEquals(cb.getCoefficient(), 1.0, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testMapsPolynomial() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"polynomial\",\"coefficient\":1.8}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "polynomial");
        Assert.assertEquals(cb.getCoefficient(), 1.8, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testMapsPolynomialIntegerCoefficient() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"polynomial\",\"coefficient\":1}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "polynomial");
        Assert.assertEquals(cb.getCoefficient(), 1.0, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testMapsLogarithmic() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"logarithmic\",\"coefficient\":1.8}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "logarithmic");
        Assert.assertEquals(cb.getCoefficient(), 1.8, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testMapsLogarithmicNoCoefficient() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"logarithmic\"}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "logarithmic");
        Assert.assertEquals(cb.getCoefficient(), 1.0, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testMapsExponential() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"exponential\",\"coefficient\":1.8}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "exponential");
        Assert.assertEquals(cb.getCoefficient(), 1.8, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapsExponentialNegativeCoefficient() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"exponential\",\"coefficient\":-1.8}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        families.add(new Family(Family.VALUE_EXPONENTIAL));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "exponential");
        Assert.assertEquals(cb.getCoefficient(), 1.8, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testMapsMissingFamily() {
        String maps = "[{\"reverse\": {\"enabled\": false}, \"quantization\" :{\"family\":\"exponential\",\"coefficient\":1.8}}]";
        params.remove("maps");
        params.add("maps", maps);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        Renderer renderer = getRenderer();
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        families.add(new Family(Family.VALUE_POLYNOMIAL));
        families.add(new Family(Family.VALUE_LOGARITHMIC));
        ctx.setMapProperties(renderer, families, 0);
        ChannelBinding cb = renderer.getChannelBindings()[0];
        Assert.assertEquals(cb.getFamily().getValue(), "linear");
        Assert.assertEquals(cb.getCoefficient(), 1.0, 0);
        Assert.assertEquals(cb.getNoiseReduction(), new Boolean(false));
    }

    @Test
    public void testGetRegionDefCtxTile()
            throws IllegalArgumentException {
        int x = 1;
        int y = 2;
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = new RegionDef(x, y, 0, 0);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 256;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 256);
        Assert.assertEquals(rdef.getX(), x * tileSize);
        Assert.assertEquals(rdef.getY(), y * tileSize);
        Assert.assertEquals(rdef.getWidth(), tileSize);
        Assert.assertEquals(rdef.getHeight(), tileSize);
    }

    @Test
    public void testGetRegionDefCtxTileWithWidthAndHeight()
            throws IllegalArgumentException {
        int x = 2;
        int y = 2;
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = new RegionDef(x, y, 64, 128);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(64, 128));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), x * 64);
        Assert.assertEquals(rdef.getY(), y * 128);
        Assert.assertEquals(rdef.getWidth(), 64);
        Assert.assertEquals(rdef.getHeight(), 128);
    }

    @Test
    public void testGetRegionDefCtxRegion()
            throws IllegalArgumentException {
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(512, 512, 256, 256);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(64, 128));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), ctx.region.getX());
        Assert.assertEquals(rdef.getY(), ctx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), ctx.region.getWidth());
        Assert.assertEquals(
                rdef.getHeight(), ctx.region.getHeight());
    }

    @Test
    public void testGetRegionDefCtxNoTileOrRegion()
    throws IllegalArgumentException {
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = null;
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(64, 128));
        when(pixelBuffer.getSizeX()).thenReturn(512);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(rdef.getX(), 0);
        Assert.assertEquals(rdef.getY(), 0);
        Assert.assertEquals(rdef.getWidth(), 512);
        Assert.assertEquals(rdef.getHeight(), 1024);
    }

//Test Truncating logic
    @Test
    public void testGetRegionDefCtxTileTruncX()
            throws IllegalArgumentException {
        int x = 1;
        int y = 0;
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = new RegionDef(x, y, 0, 0);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), x * tileSize);
        Assert.assertEquals(rdef.getY(), y * tileSize);
        Assert.assertEquals(rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(rdef.getHeight(), tileSize);
    }

    @Test
    public void testGetRegionDefCtxTileTruncY()
            throws IllegalArgumentException, ServerError {
        int x = 0;
        int y = 1;
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = new RegionDef(x, y, 0, 0);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), x * tileSize);
        Assert.assertEquals(rdef.getY(), y * tileSize);
        Assert.assertEquals(rdef.getWidth(), tileSize);
        Assert.assertEquals(rdef.getHeight(), 1024 - rdef.getY());
    }

    @Test
    public void testGetRegionDefCtxTileTruncXY()
            throws IllegalArgumentException, ServerError {
        int x = 1;
        int y = 1;
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = new RegionDef(x, y, 0, 0);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), x * tileSize);
        Assert.assertEquals(rdef.getY(), y * tileSize);
        Assert.assertEquals(rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(rdef.getHeight(), 1024 - rdef.getY());
    }

    @Test
    public void testGetRegionDefCtxRegionTruncX()
            throws IllegalArgumentException {
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(800, 100, 300, 400);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(1024, 1024));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), ctx.region.getX());
        Assert.assertEquals(rdef.getY(), ctx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(
                rdef.getHeight(), ctx.region.getHeight());
    }

    @Test
    public void testGetRegionDefCtxRegionTruncY()
            throws IllegalArgumentException {
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(100, 800, 300, 400);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(1024, 1024));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), ctx.region.getX());
        Assert.assertEquals(rdef.getY(), ctx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), ctx.region.getWidth());
        Assert.assertEquals(
                rdef.getHeight(), 1024 - rdef.getY());
    }

    @Test
    public void testGetRegionDefCtxRegionTruncXY()
            throws IllegalArgumentException, ServerError {
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(800, 800, 300, 400);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(1024, 1024));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        RegionDef rdef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(rdef.getX(), ctx.region.getX());
        Assert.assertEquals(rdef.getY(), ctx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(
                rdef.getHeight(), 1024 - rdef.getY());
    }

//Test Flipping
    @Test
    public void testFlipRegionDefFlipH() {
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256, 256));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(100, 200, 300, 400);
        ctx.flipHorizontal = true;
        ctx.flipVertical = false;
        RegionDef regionDef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(regionDef.getX(), 624);
        Assert.assertEquals(regionDef.getY(), 200);
        Assert.assertEquals(regionDef.getWidth(), 300);
        Assert.assertEquals(regionDef.getHeight(), 400);
    }

    @Test
    public void testFlipRegionDefFlipV() {
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256, 256));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(100, 200, 300, 400);
        ctx.flipHorizontal = false;
        ctx.flipVertical = true;
        RegionDef regionDef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(regionDef.getX(), 100);
        Assert.assertEquals(regionDef.getY(), 424);
        Assert.assertEquals(regionDef.getWidth(), 300);
        Assert.assertEquals(regionDef.getHeight(), 400);
    }

    @Test
    public void testFlipRegionDefFlipHV() {
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256, 256));
        when(pixelBuffer.getSizeX()).thenReturn(1024);
        when(pixelBuffer.getSizeY()).thenReturn(1024);
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        ctx.region = new RegionDef(100, 200, 300, 400);
        ctx.flipHorizontal = true;
        ctx.flipVertical = true;
        RegionDef regionDef = ctx.getRegionDef(pixelBuffer, 1024);
        Assert.assertEquals(regionDef.getX(), 624);
        Assert.assertEquals(regionDef.getY(), 424);
        Assert.assertEquals(regionDef.getWidth(), 300);
        Assert.assertEquals(regionDef.getHeight(), 400);
    }

    @Test
    public void testFlipRegionDefMirorXEdge() throws ServerError{
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        // Tile 0, 0
        ctx.tile = null;
        ctx.region = new RegionDef(0, 0, 1024, 1024);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(512, 512));
        when(pixelBuffer.getSizeX()).thenReturn(768);
        when(pixelBuffer.getSizeY()).thenReturn(768);
        ctx.flipHorizontal = true;
        ctx.flipVertical = false;
        RegionDef regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 768);
        Assert.assertEquals(regionDef.getHeight(), 768);

        // Tile 1, 0
        ctx.region = new RegionDef(512, 0, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        ctx.region = new RegionDef(0, 512, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 512);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        ctx.region = new RegionDef(512, 512, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 512);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testFlipRegionDefMirorYEdge() throws ServerError{
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        // Tile 0, 0
        ctx.tile = null;
        ctx.region = new RegionDef(0, 0, 512, 512);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(512, 512));
        when(pixelBuffer.getSizeX()).thenReturn(768);
        when(pixelBuffer.getSizeY()).thenReturn(768);
        ctx.flipVertical = true;
        RegionDef regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        ctx.region = new RegionDef(512, 0, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 512);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        ctx.region = new RegionDef(0, 512, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        ctx.region = new RegionDef(512, 512, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 512);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testFlipRegionDefMirorXYEdge() throws ServerError{
        ImageRegionCtx ctx = new ImageRegionCtx(params, "");
        ctx.tile = null;
        // Tile 0, 0
        ctx.region = new RegionDef(0, 0, 512, 512);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(512, 512));
        when(pixelBuffer.getSizeX()).thenReturn(768);
        when(pixelBuffer.getSizeY()).thenReturn(768);
        ctx.flipHorizontal = true;
        ctx.flipVertical = true;
        RegionDef regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        ctx.region = new RegionDef(512, 0, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        ctx.region = new RegionDef(0, 512, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        ctx.region = new RegionDef(512, 512, 512, 512);
        regionDef = ctx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }
}
