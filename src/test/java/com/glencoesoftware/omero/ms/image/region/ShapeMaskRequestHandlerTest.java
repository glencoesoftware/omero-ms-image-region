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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;

import javax.imageio.ImageIO;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import ome.xml.model.primitives.Color;

public class ShapeMaskRequestHandlerTest {

    private ShapeMaskRequestHandler handler = null;

    private void assertImage(BufferedImage image, int width, int height) {
        Assert.assertNotNull(image);
        Assert.assertEquals(image.getWidth(), width);
        Assert.assertEquals(image.getHeight(), height);
    }

    @BeforeMethod
    public void setUp() {
        MultiMap params = new CaseInsensitiveHeaders();

        params.add("shapeId", "1");
        params.add("color", "0");
        params.add("flip", "");

        handler = new ShapeMaskRequestHandler(new ShapeMaskCtx(params, ""), "", "");
    }

    @Test
    public void testRenderShapeMaskByteAligned() throws IOException {
        Color fillColor = new Color(255, 0, 0, 255);
        // 8 by 2 grid alternating bits
        byte[] bytes = new byte[] { 0x55, 0x55 };
        int width = 8;
        int height = 2;
        byte[] png = handler.renderShapeMask(fillColor, bytes, width, height);
        Assert.assertNotNull(png);
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(png));
        assertImage(image, width, height);
    }

    @Test
    public void testRenderShapeMaskNotByteAligned() throws IOException {
        Color fillColor = new Color(255, 0, 0, 255);
        // 4 by 4 grid alternating bits
        byte[] bytes = new byte[] { 0x55, 0x55 };
        int width = 4;
        int height = 4;
        byte[] png = handler.renderShapeMask(fillColor, bytes, width, height);
        Assert.assertNotNull(png);
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(png));
        assertImage(image, width, height);
    }


    private void testFlip(
            byte[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        byte[] flipped = ShapeMaskRequestHandler.flip(
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

    private void testAllFlips(byte[] src, int sizeX, int sizeY) {
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
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipOddSquare(){
        int sizeX = 5;
        int sizeY = 5;
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipWideRectangle() {
        int sizeX = 7;
        int sizeY = 4;
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipTallRectangle() {
        int sizeX = 4;
        int sizeY = 7;
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleWidthRectangle() {
        int sizeX = 7;
        int sizeY = 1;
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleHeightRectangle() {
        int sizeX = 1;
        int sizeY = 7;
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleEntry() {
        int sizeX = 1;
        int sizeY = 1;
        byte[] src = new byte[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = (byte) n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipNullImage() {
        byte[] nullArray = null;
        ShapeMaskRequestHandler.flip(nullArray, 4, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipZeroXImage() {
        byte[] src = {1};
        ShapeMaskRequestHandler.flip(src, 0, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipZeroYImage() {
        byte[] src = {1};
        ShapeMaskRequestHandler.flip(src, 4, 0, true, true);
    }

}
