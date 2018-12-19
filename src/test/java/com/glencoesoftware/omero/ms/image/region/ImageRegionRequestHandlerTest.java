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

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

public class ImageRegionRequestHandlerTest {

    @BeforeMethod
    public void setUp() {
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
}
