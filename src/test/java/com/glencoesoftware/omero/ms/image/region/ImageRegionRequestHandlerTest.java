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

import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

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

    @Before
    public void setUp() {
        MultiMap params = new CaseInsensitiveHeaders();

        params.add("imageId", "1");
        params.add("theZ", "0");
        params.add("theT", "0");
        params.add("m", "rgb");

        imageRegionCtx = new ImageRegionCtx(params, "");
        reqHandler = new ImageRegionRequestHandler(imageRegionCtx,
                new ArrayList<Family>(),
                new ArrayList<RenderingModel>(),
                null, //LutProvider lutProvider,
                null, //LocalCompress compSrv,
                1024, //maxTileLength,
                null  //PixelsService
        );
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
        when(pixelBuffer.getSizeX()).thenReturn(256);
        when(pixelBuffer.getSizeY()).thenReturn(512);
        RegionDef rdef = imageRegionCtx.getRegionDef(pixelBuffer, 2048);
        Assert.assertEquals(rdef.getX(), x);
        Assert.assertEquals(rdef.getY(), y);
        Assert.assertEquals(rdef.getWidth(), 256 - rdef.getX());
        Assert.assertEquals(rdef.getHeight(), 512 - rdef.getY());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlipNullImage() {
        int[] nullArray = null;
        reqHandler.flip(nullArray, 4, 4, true, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlipZeroXImage() {
        int[] src = {1};
        reqHandler.flip(src, 0, 4, true, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFlipZeroYImage() {
        int[] src = {1};
        reqHandler.flip(src, 4, 0, true, true);
    }

    private void testFlip(
            int[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        int[] flipped = reqHandler.flip(
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
}
