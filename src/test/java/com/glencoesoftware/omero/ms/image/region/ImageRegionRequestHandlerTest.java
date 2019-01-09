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

import java.awt.Dimension;
import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import ome.model.core.Pixels;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import omeis.providers.re.data.RegionDef;


public class ImageRegionRequestHandlerTest {

    private ImageRegionCtx imageRegionCtx;

    private ImageRegionRequestHandler reqHandler;

    @BeforeMethod
    public void setUp() {
        MultiMap params = new CaseInsensitiveHeaders();

        params.add("imageId", "1");
        params.add("theZ", "0");
        params.add("theT", "0");
        params.add("m", "rgb");

        imageRegionCtx = new ImageRegionCtx(params, "");
        reqHandler = new ImageRegionRequestHandler(
                imageRegionCtx,
                null, //ApplicationContext context,
                new ArrayList<Family>(),
                new ArrayList<RenderingModel>(),
                null, //LutProvider lutProvider,
                null, //LocalCompress compSrv,
                null); //PixelsService pixService);
    }

    private void testMirror(
            int[] src, int sizeX, int sizeY, boolean mirrorX, boolean mirrorY) {
        int[] mirrored = ImageRegionRequestHandler.mirror(
                src, sizeX, sizeY, mirrorX, mirrorY);
        for (int n = 0; n < sizeX*sizeY; n++){
            int new_col;
            if (mirrorX) {
                int old_col = n % sizeX;
                new_col = sizeX - 1 - old_col;
            }
            else {
                new_col = n % sizeX;
            }
            int new_row;
            if (mirrorY) {
                int old_row = n / sizeX;
                new_row = sizeY - 1 - old_row;
            }
            else {
                new_row = n / sizeX;
            }
            Assert.assertEquals(mirrored[new_row * sizeX + new_col], n);
        }
    }

    private void testAllMirrors(int[] src, int sizeX, int sizeY) {
        boolean mirrorX = false;
        boolean mirrorY = true;
        testMirror(src, sizeX, sizeY, mirrorX, mirrorY);

        mirrorX = true; 
        mirrorY = false;
        testMirror(src, sizeX, sizeY, mirrorX, mirrorY);

        mirrorX = true; 
        mirrorY = true;
        testMirror(src, sizeX, sizeY, mirrorX, mirrorY);
    }

    @Test
    public void testMirrorEvenSquare2() {
        int sizeX = 4;
        int sizeY = 4;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorOddSquare(){
        int sizeX = 5;
        int sizeY = 5;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorWideRectangle() {
        int sizeX = 7;
        int sizeY = 4;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorTallRectangle() {
        int sizeX = 4;
        int sizeY = 7;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorSingleWidthRectangle() {
        int sizeX = 7;
        int sizeY = 1;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorSingleHeightRectangle() {
        int sizeX = 1;
        int sizeY = 7;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorSingleEntry() {
        int sizeX = 1;
        int sizeY = 1;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMirrorNullImage() {
        ImageRegionRequestHandler.mirror(null, 4, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMirrorZeroXImage() {
        int[] src = {1};
        ImageRegionRequestHandler.mirror(src, 0, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMirrorZeroYImage() {
        int[] src = {1};
        ImageRegionRequestHandler.mirror(src, 4, 0, true, true);
    }

    @Test
    public void testMirrorRegionDefMirrorNothing() {
        RegionDef regionDef = new RegionDef(0, 0, 256, 256);
        Pixels pixels = new Pixels();
        pixels.setSizeX(1024);
        pixels.setSizeY(1024);
        reqHandler.mirrorRegionDef(pixels, new Dimension(256, 256), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }


    @Test
    public void testMirrorRegionDefMirrorX() {
        RegionDef regionDef = new RegionDef(0, 0, 256, 256);
        Pixels pixels = new Pixels();
        pixels.setSizeX(1024);
        pixels.setSizeY(1024);
        imageRegionCtx.mirrorX = true;
        reqHandler.mirrorRegionDef(pixels, new Dimension(256, 256), regionDef);
        Assert.assertEquals(regionDef.getX(), 768);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testMirrorRegionDefMirrorY() {
        RegionDef regionDef = new RegionDef(0, 0, 256, 256);
        Pixels pixels = new Pixels();
        pixels.setSizeX(1024);
        pixels.setSizeY(1024);
        imageRegionCtx.mirrorY = true;
        reqHandler.mirrorRegionDef(pixels, new Dimension(256, 256), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 768);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testMirrorRegionDefMirrorXY() {
        RegionDef regionDef = new RegionDef(0, 0, 256, 256);
        Pixels pixels = new Pixels();
        pixels.setSizeX(1024);
        pixels.setSizeY(1024);
        imageRegionCtx.mirrorX = true;
        imageRegionCtx.mirrorY = true;
        reqHandler.mirrorRegionDef(pixels, new Dimension(256, 256), regionDef);
        Assert.assertEquals(regionDef.getX(), 768);
        Assert.assertEquals(regionDef.getY(), 768);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testMirrorRegionDefMirorXEdge() {
        // Tile 0, 0
        RegionDef regionDef = new RegionDef(0, 0, 512, 512);
        Pixels pixels = new Pixels();
        pixels.setSizeX(768);
        pixels.setSizeY(768);
        imageRegionCtx.mirrorX = true;
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        regionDef = new RegionDef(512, 0, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        regionDef = new RegionDef(0, 512, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 512);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);
        // ^^ Will be confined to the image sizeY by the rendering operation

        // Tile 1, 1
        regionDef = new RegionDef(512, 512, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 512);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);
        // ^^ Will be confined to the image sizeY by the rendering operation
    }

    @Test
    public void testMirrorRegionDefMirorYEdge() {
        // Tile 0, 0
        RegionDef regionDef = new RegionDef(0, 0, 512, 512);
        Pixels pixels = new Pixels();
        pixels.setSizeX(768);
        pixels.setSizeY(768);
        imageRegionCtx.mirrorY = true;
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        regionDef = new RegionDef(512, 0, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 512);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        // ^^ Will be confined to the image sizeX by the rendering operation
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        regionDef = new RegionDef(0, 512, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        regionDef = new RegionDef(512, 512, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 512);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        // ^^ Will be confined to the image sizeX by the rendering operation
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testMirrorRegionDefMirorXYEdge() {
        // Tile 0, 0
        RegionDef regionDef = new RegionDef(0, 0, 512, 512);
        Pixels pixels = new Pixels();
        pixels.setSizeX(768);
        pixels.setSizeY(768);
        imageRegionCtx.mirrorX = true;
        imageRegionCtx.mirrorY = true;
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        regionDef = new RegionDef(512, 0, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        regionDef = new RegionDef(0, 512, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        regionDef = new RegionDef(512, 512, 512, 512);
        reqHandler.mirrorRegionDef(pixels, new Dimension(512, 512), regionDef);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

}
