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

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.lang.Integer;
import java.awt.Dimension;

import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;

import org.testng.annotations.Test;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import omeis.providers.re.data.RegionDef;
import ome.io.nio.PixelBuffer;
import ome.model.core.Pixels;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;

import omero.ServerError;



public class ImageRegionRequestHandlerTest {

    private ImageRegionCtx imageRegionCtx = null;
    private ImageRegionRequestHandler reqHandler = null;

    @BeforeMethod
    public void setUp() {
        MultiMap params = new CaseInsensitiveHeaders();

        params.add("imageId", "1");
        params.add("theZ", "0");
        params.add("theT", "0");
        params.add("tile", "0,0,0");
        params.add("region","0,0,1,1");
        params.add("c", "-1|0:65535$0000FF,2|1755:51199$00FF00,3|3218:26623$FF0000");
        params.add("m", "");
        params.add("mirror", "");

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

    private void testMirror(int[] src,
        int sizeX,
        int sizeY,
        boolean mirrorX,
        boolean mirrorY) {
        int[] mirrored = ImageRegionRequestHandler.mirror(src, sizeX, sizeY, mirrorX, mirrorY);
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
            Assert.assertEquals(mirrored[new_row*sizeX + new_col], n);
        }
    }

    private void testAllMirrors(int[] src,
        int sizeX,
        int sizeY) {
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
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorOddSquare(){
        int sizeX = 5;
        int sizeY = 5;
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorWideRectangle() {
        int sizeX = 7;
        int sizeY = 4;
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorTallRectangle() {
        int sizeX = 4;
        int sizeY = 7;
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorSingleWidthRectangle() {
        int sizeX = 7;
        int sizeY = 1;
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorSingleHeightRectangle() {
        int sizeX = 1;
        int sizeY = 7;
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test
    public void testMirrorSingleEntry() {
        int sizeX = 1;
        int sizeY = 1;
        int[] src = new int[sizeX*sizeY];
        for (int n = 0; n < sizeX*sizeY; n++){
            src[n] = n;
        }
        testAllMirrors(src, sizeX, sizeY);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testMirrorNullImage() {
        ImageRegionRequestHandler.mirror(null, 4, 4, true, true);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testMirrorZeroXImage() {
        int[] src = {1};
        ImageRegionRequestHandler.mirror(src, 0, 4, true, true);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testMirrorZeroYImage() {
        int[] src = {1};
        ImageRegionRequestHandler.mirror(src, 4, 0, true, true);
    }

    @Test
    public void testGetMirroredRegionDef() {
        RegionDef originalRegion = new RegionDef(1,1,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1024,
            1024,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            false,
            false);
        Assert.assertEquals(256, finalDef.getX());
        Assert.assertEquals(256, finalDef.getY());
        Assert.assertEquals(256, finalDef.getWidth());
        Assert.assertEquals(256, finalDef.getHeight());
    }

    @Test
    public void testGetMirroredRegionDefX() {
        RegionDef originalRegion = new RegionDef(1,1,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1024,
            1024,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            true,
            false);
        Assert.assertEquals(512, finalDef.getX());
        Assert.assertEquals(256, finalDef.getY());
        Assert.assertEquals(256, finalDef.getWidth());
        Assert.assertEquals(256, finalDef.getHeight());
    }

    @Test
    public void testGetMirroredRegionDefY() {
        RegionDef originalRegion = new RegionDef(1,1,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1024,
            1024,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            false,
            true);
        Assert.assertEquals(finalDef.getX(), 256);
        Assert.assertEquals(finalDef.getY(), 512);
        Assert.assertEquals(finalDef.getWidth(), 256);
        Assert.assertEquals(finalDef.getHeight(), 256);
    }

    @Test
    public void testGetMirroredRegionDefXY() {
        RegionDef originalRegion = new RegionDef(1,1,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1024,
            1024,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            true,
            true);
        Assert.assertEquals(finalDef.getX(), 512);
        Assert.assertEquals(finalDef.getY(), 512);
        Assert.assertEquals(finalDef.getWidth(), 256);
        Assert.assertEquals(finalDef.getHeight(), 256);
    }

    @Test
    public void testGetMirroredRegionDefXEdge() {
        RegionDef originalRegion = new RegionDef(0,1,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1023,
            1024,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            true,
            false);
        Assert.assertEquals(finalDef.getX(), 768);
        Assert.assertEquals(finalDef.getY(), 256);
        Assert.assertEquals(finalDef.getWidth(), 255);
        Assert.assertEquals(finalDef.getHeight(), 256);
    }

    @Test
    public void testGetMirroredRegionDefYEdge() {
        RegionDef originalRegion = new RegionDef(1,0,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1024,
            1023,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            false,
            true);
        Assert.assertEquals(finalDef.getX(), 256);
        Assert.assertEquals(finalDef.getY(), 768);
        Assert.assertEquals(finalDef.getWidth(), 256);
        Assert.assertEquals(finalDef.getHeight(), 255);
    }

    @Test
    public void testGetMirroredRegionDefXYEdge() {
        RegionDef originalRegion = new RegionDef(0,0,256,256);
        RegionDef finalDef = reqHandler.getMirroredRegionDef(
            1023,
            1023,
            originalRegion.getWidth(),
            originalRegion.getHeight(),
            originalRegion.getX(),
            originalRegion.getY(),
            true,
            true);
        Assert.assertEquals(finalDef.getX(), 768);
        Assert.assertEquals(finalDef.getY(), 768);
        Assert.assertEquals(finalDef.getWidth(), 255);
        Assert.assertEquals(finalDef.getHeight(), 255);
    }

    @Test
    public void testGetRegionDefCtxTile()
        throws IllegalArgumentException, ServerError {
        int x = 2;
        int y = 2;
        imageRegionCtx.tile = new RegionDef(x, y, 0, 0);
        Pixels pixels = mock(Pixels.class);
        when(pixels.getSizeX()).thenReturn(new Integer(1024));
        when(pixels.getSizeY()).thenReturn(new Integer(1024));
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSide = 256;
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(tileSide, tileSide));
        RegionDef rdef = reqHandler.getRegionDef(pixels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), x*tileSide);
        Assert.assertEquals(rdef.getX(), y*tileSide);
        Assert.assertEquals(rdef.getWidth(), tileSide);
        Assert.assertEquals(rdef.getHeight(), tileSide);
    }

    @Test
    public void testGetRegionDefCtxRegion()
        throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = new RegionDef(512, 512, 256, 256);
        Pixels pixels = mock(Pixels.class);
        when(pixels.getSizeX()).thenReturn(new Integer(1024));
        when(pixels.getSizeY()).thenReturn(new Integer(1024));
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256,256));
        RegionDef rdef = reqHandler.getRegionDef(pixels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), imageRegionCtx.region.getX());
        Assert.assertEquals(rdef.getX(), imageRegionCtx.region.getY());
        Assert.assertEquals(rdef.getWidth(), imageRegionCtx.region.getWidth());
        Assert.assertEquals(rdef.getHeight(), imageRegionCtx.region.getHeight());
    }

    @Test
    public void testGetRegionDefCtxNoTileOrRegion()
        throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = null;
        Pixels pixels = mock(Pixels.class);
        when(pixels.getSizeX()).thenReturn(new Integer(1024));
        when(pixels.getSizeY()).thenReturn(new Integer(1024));
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256,256));
        RegionDef rdef = reqHandler.getRegionDef(pixels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), 0);
        Assert.assertEquals(rdef.getX(), 0);
        Assert.assertEquals(rdef.getWidth(), 1024);
        Assert.assertEquals(rdef.getHeight(), 1024);
    }

}
