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

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import ome.io.nio.PixelBuffer;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import omeis.providers.re.data.RegionDef;

import omero.ServerError;

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
                new ArrayList<Family>(),
                new ArrayList<RenderingModel>(),
                null, //LutProvider lutProvider,
                null, //LocalCompress compSrv,
                null, //PixelsService pixService,
                1024, //maxTileLength,
                "", //ngffDir
                null); //tiledbUtils);
    }

    private void testFlip(
            int[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        int[] flipped = ImageRegionRequestHandler.flip(
                src, sizeX, sizeY, flipHorizontal, flipVertical);
        for (int n = 0; n < sizeX * sizeY; n++){
            int new_col;
            if (flipHorizontal) {
                int old_col = n % sizeX;
                new_col = sizeX - 1 - old_col;
            } else {
                new_col = n % sizeX;
            }
            int new_row;
            if (flipVertical) {
                int old_row = n / sizeX;
                new_row = sizeY - 1 - old_row;
            } else {
                new_row = n / sizeX;
            }
            Assert.assertEquals(flipped[new_row * sizeX + new_col], n);
        }
    }

    private void testAllFlips(int[] src, int sizeX, int sizeY) {
        boolean flipHorizontal = false;
        boolean flipVertical = true;
        testFlip(src, sizeX, sizeY, flipHorizontal, flipVertical);

        flipHorizontal = true;
        flipVertical = false;
        testFlip(src, sizeX, sizeY, flipHorizontal, flipVertical);

        flipHorizontal = true;
        flipVertical = true;
        testFlip(src, sizeX, sizeY, flipHorizontal, flipVertical);
    }

    @Test
    public void testFlipEvenSquare2() {
        int sizeX = 4;
        int sizeY = 4;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipOddSquare(){
        int sizeX = 5;
        int sizeY = 5;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipWideRectangle() {
        int sizeX = 7;
        int sizeY = 4;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipTallRectangle() {
        int sizeX = 4;
        int sizeY = 7;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleWidthRectangle() {
        int sizeX = 7;
        int sizeY = 1;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleHeightRectangle() {
        int sizeX = 1;
        int sizeY = 7;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleEntry() {
        int sizeX = 1;
        int sizeY = 1;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipNullImage() {
        int[] nullArray = null;
        ImageRegionRequestHandler.flip(nullArray, 4, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipZeroXImage() {
        int[] src = {1};
        ImageRegionRequestHandler.flip(src, 0, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipZeroYImage() {
        int[] src = {1};
        ImageRegionRequestHandler.flip(src, 4, 0, true, true);
    }

    @Test
    public void testGetRegionDefCtxTile()
            throws IllegalArgumentException, ServerError {
        int x = 2;
        int y = 2;
        imageRegionCtx.tile = new RegionDef(x, y, 0, 0);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 256;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        RegionDef rdef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), x * tileSize);
        Assert.assertEquals(rdef.getY(), y * tileSize);
        Assert.assertEquals(rdef.getWidth(), tileSize);
        Assert.assertEquals(rdef.getHeight(), tileSize);
    }

    @Test
    public void testGetRegionDefCtxTileWithWidthAndHeight()
            throws IllegalArgumentException, ServerError {
        int x = 2;
        int y = 2;
        imageRegionCtx.tile = new RegionDef(x, y, 64, 128);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(64, 128));
        RegionDef rdef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), x * 64);
        Assert.assertEquals(rdef.getY(), y * 128);
        Assert.assertEquals(rdef.getWidth(), 64);
        Assert.assertEquals(rdef.getHeight(), 128);
    }

    @Test
    public void testGetRegionDefCtxRegion()
            throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = new RegionDef(512, 512, 256, 256);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        RegionDef rdef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), imageRegionCtx.region.getX());
        Assert.assertEquals(rdef.getY(), imageRegionCtx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), imageRegionCtx.region.getWidth());
        Assert.assertEquals(
                rdef.getHeight(), imageRegionCtx.region.getHeight());
    }

    @Test
    public void testGetRegionDefCtxNoTileOrRegion()
    throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = null;
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), 0);
        Assert.assertEquals(rdef.getY(), 0);
        Assert.assertEquals(rdef.getWidth(), 1024);
        Assert.assertEquals(rdef.getHeight(), 1024);
    }

//Test Truncating logic
    @Test
    public void testGetRegionDefCtxTileTruncX()
            throws IllegalArgumentException, ServerError {
        int x = 1;
        int y = 0;
        imageRegionCtx.tile = new RegionDef(x, y, 0, 0);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
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
        imageRegionCtx.tile = new RegionDef(x, y, 0, 0);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
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
        imageRegionCtx.tile = new RegionDef(x, y, 0, 0);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), x * tileSize);
        Assert.assertEquals(rdef.getY(), y * tileSize);
        Assert.assertEquals(rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(rdef.getHeight(), 1024 - rdef.getY());
    }

    @Test
    public void testGetRegionDefCtxRegionTruncX()
            throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = new RegionDef(800, 100, 300, 400);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), imageRegionCtx.region.getX());
        Assert.assertEquals(rdef.getY(), imageRegionCtx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(
                rdef.getHeight(), imageRegionCtx.region.getHeight());
    }

    @Test
    public void testGetRegionDefCtxRegionTruncY()
            throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = new RegionDef(100, 800, 300, 400);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), imageRegionCtx.region.getX());
        Assert.assertEquals(rdef.getY(), imageRegionCtx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), imageRegionCtx.region.getWidth());
        Assert.assertEquals(
                rdef.getHeight(), 1024 - rdef.getY());
    }

    @Test
    public void testGetRegionDefCtxRegionTruncXY()
            throws IllegalArgumentException, ServerError {
        imageRegionCtx.tile = null;
        imageRegionCtx.region = new RegionDef(800, 800, 300, 400);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), imageRegionCtx.region.getX());
        Assert.assertEquals(rdef.getY(), imageRegionCtx.region.getY());
        Assert.assertEquals(
                rdef.getWidth(), 1024 - rdef.getX());
        Assert.assertEquals(
                rdef.getHeight(), 1024 - rdef.getY());
    }

//Test Flipping
    @Test
    public void testFlipRegionDefFlipH() throws ServerError{
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256, 256));
        imageRegionCtx.region = new RegionDef(100, 200, 300, 400);
        imageRegionCtx.flipHorizontal = true;
        imageRegionCtx.flipVertical = false;
        RegionDef regionDef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 624);
        Assert.assertEquals(regionDef.getY(), 200);
        Assert.assertEquals(regionDef.getWidth(), 300);
        Assert.assertEquals(regionDef.getHeight(), 400);
    }

    @Test
    public void testFlipRegionDefFlipV() throws ServerError{
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256, 256));
        imageRegionCtx.region = new RegionDef(100, 200, 300, 400);
        imageRegionCtx.flipHorizontal = false;
        imageRegionCtx.flipVertical = true;
        RegionDef regionDef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 100);
        Assert.assertEquals(regionDef.getY(), 424);
        Assert.assertEquals(regionDef.getWidth(), 300);
        Assert.assertEquals(regionDef.getHeight(), 400);
    }

    @Test
    public void testFlipRegionDefFlipHV() throws ServerError{
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(256, 256));
        imageRegionCtx.region = new RegionDef(100, 200, 300, 400);
        imageRegionCtx.flipHorizontal = true;
        imageRegionCtx.flipVertical = true;
        RegionDef regionDef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 624);
        Assert.assertEquals(regionDef.getY(), 424);
        Assert.assertEquals(regionDef.getWidth(), 300);
        Assert.assertEquals(regionDef.getHeight(), 400);
    }

    @Test
    public void testFlipRegionDefMirorXEdge() throws ServerError{
        // Tile 0, 0
        imageRegionCtx.region = new RegionDef(0, 0, 1024, 1024);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 768, 768 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(512, 512));
        imageRegionCtx.flipHorizontal = true;
        imageRegionCtx.flipVertical = false;
        RegionDef regionDef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 768);
        Assert.assertEquals(regionDef.getHeight(), 768);

        // Tile 1, 0
        imageRegionCtx.region = new RegionDef(512, 0, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        imageRegionCtx.region = new RegionDef(0, 512, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 512);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        imageRegionCtx.region = new RegionDef(512, 512, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 512);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testFlipRegionDefMirorYEdge() throws ServerError{
        // Tile 0, 0
        imageRegionCtx.region = new RegionDef(0, 0, 512, 512);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 768, 768 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(512, 512));
        imageRegionCtx.flipVertical = true;
        RegionDef regionDef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        imageRegionCtx.region = new RegionDef(512, 0, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 512);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        imageRegionCtx.region = new RegionDef(0, 512, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        imageRegionCtx.region = new RegionDef(512, 512, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 512);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

    @Test
    public void testFlipRegionDefMirorXYEdge() throws ServerError{
        // Tile 0, 0
        imageRegionCtx.region = new RegionDef(0, 0, 512, 512);
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 768, 768 });
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(512, 512));
        imageRegionCtx.flipHorizontal = true;
        imageRegionCtx.flipVertical = true;
        RegionDef regionDef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 1, 0
        imageRegionCtx.region = new RegionDef(512, 0, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 256);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 512);

        // Tile 0, 1
        imageRegionCtx.region = new RegionDef(0, 512, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 256);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 512);
        Assert.assertEquals(regionDef.getHeight(), 256);

        // Tile 1, 1
        imageRegionCtx.region = new RegionDef(512, 512, 512, 512);
        regionDef = reqHandler.getRegionDef(resolutionLevels, pixelBuffer);
        Assert.assertEquals(regionDef.getX(), 0);
        Assert.assertEquals(regionDef.getY(), 0);
        Assert.assertEquals(regionDef.getWidth(), 256);
        Assert.assertEquals(regionDef.getHeight(), 256);
    }

//Test Resolution Selection
    @Test
    public void testSelectResolution()
            throws IllegalArgumentException, ServerError {
        int x = 100;
        int y = 200;
        imageRegionCtx.tile = null;
        imageRegionCtx.region = new RegionDef(x, y, 400, 500);
        imageRegionCtx.resolution = 1;
        List<List<Integer>> resolutionLevels = new ArrayList<List<Integer>>();
        List<Integer> resolutionLevel =
                Arrays.asList(new Integer[] { 1024, 1024 });
        resolutionLevels.add(resolutionLevel);
        resolutionLevel = Arrays.asList(new Integer[] {256, 512});
        resolutionLevels.add(resolutionLevel);
        PixelBuffer pixelBuffer = mock(PixelBuffer.class);
        int tileSize = 800;
        when(pixelBuffer.getTileSize())
            .thenReturn(new Dimension(tileSize, tileSize));
        RegionDef rdef = reqHandler.getRegionDef(
                resolutionLevels, pixelBuffer);
        Assert.assertEquals(rdef.getX(), x);
        Assert.assertEquals(rdef.getY(), y);
        Assert.assertEquals(rdef.getWidth(), 256 - rdef.getX());
        Assert.assertEquals(rdef.getHeight(), 512 - rdef.getY());
    }

}
