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

import org.testng.annotations.Test;

import org.testng.Assert;

import ome.xml.model.primitives.Color;

public class ShapeMaskRequestHandlerTest {

    @Test
    public void testRenderShapeMaskByteAligned() throws IOException {
        ShapeMaskRequestHandler handler = new ShapeMaskRequestHandler(null);
        Color fillColor = new Color(255, 0, 0, 255);
        // 8 by 2 grid alternating bits
        byte[] bytes = new byte[] { 0x55, 0x55 };
        int width = 8;
        int height = 2;
        byte[] png = handler.renderShapeMaskByteAligned(
                fillColor, bytes, width, height);
        Assert.assertNotNull(png);
    }

    @Test
    public void testRenderShapeMaskNotByteAligned() throws IOException {
        ShapeMaskRequestHandler handler = new ShapeMaskRequestHandler(null);
        Color fillColor = new Color(255, 0, 0, 255);
        // 4 by 4 grid alternating bits
        byte[] bytes = new byte[] { 0x55, 0x55 };
        int width = 4;
        int height = 4;
        byte[] png = handler.renderShapeMaskByteAligned(
                fillColor, bytes, width, height);
        Assert.assertNotNull(png);
    }

}
