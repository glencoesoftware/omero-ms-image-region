/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.bc.zarr.ZarrArray;

import brave.Tracing;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.model.core.Pixels;
import ome.util.PixelData;
import ucar.ma2.InvalidRangeException;
import zipkin2.reporter.Reporter;

public class ZarrPixelBufferTest extends AbstractZarrPixelBufferTest {

    @Test
    public void testGetChunks() throws IOException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            int[][] chunks = zpbuf.getChunks();
            int[][] expectedChunks = new int[][] {
                new int[] {1, 1, 1, 512, 1024},
                new int[] {1, 1, 1, 256, 1024},
                new int[] {1, 1, 1, 128, 512}
            };
            for(int i = 0; i < chunks.length; i++) {
                Assert.assertTrue(Arrays.equals(
                        chunks[i], expectedChunks[i]));
            }
        }
    }

    @Test
    public void testGetDatasets() throws IOException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16",
                resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            List<Map<String,String>> datasets = zpbuf.getDatasets();
            List<Map<String,String>> expectedDatasets = getDatasets(3);
            for (int i = 0; i < datasets.size(); i++) {
                Assert.assertEquals(datasets.get(i), expectedDatasets.get(i));
            }
        }
    }

    @Test
    public void testGetResolutionDescriptions() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            List<List<Integer>> expected = new ArrayList<List<Integer>>();
            expected.add(Arrays.asList(new Integer[] {2048, 512}));
            expected.add(Arrays.asList(new Integer[] {1024, 256}));
            expected.add(Arrays.asList(new Integer[] {512, 128}));
            Assert.assertEquals(resolutions, zpbuf.getResolutionLevels());
            Assert.assertEquals(expected, zpbuf.getResolutionDescriptions());

            zpbuf.setResolutionLevel(0);
            Assert.assertEquals(zpbuf.getSizeT(), 1);
            Assert.assertEquals(zpbuf.getSizeC(), 2);
            Assert.assertEquals(zpbuf.getSizeZ(), 3);
            Assert.assertEquals(zpbuf.getSizeY(), 128);
            Assert.assertEquals(zpbuf.getSizeX(), 512);
            zpbuf.setResolutionLevel(1);
            Assert.assertEquals(zpbuf.getSizeT(), 1);
            Assert.assertEquals(zpbuf.getSizeC(), 2);
            Assert.assertEquals(zpbuf.getSizeZ(), 3);
            Assert.assertEquals(zpbuf.getSizeY(), 256);
            Assert.assertEquals(zpbuf.getSizeX(), 1024);
            zpbuf.setResolutionLevel(2);
            Assert.assertEquals(zpbuf.getSizeT(), 1);
            Assert.assertEquals(zpbuf.getSizeC(), 2);
            Assert.assertEquals(zpbuf.getSizeZ(), 3);
            Assert.assertEquals(zpbuf.getSizeY(), 512);
            Assert.assertEquals(zpbuf.getSizeX(), 2048);
        }
    }

    @Test
    public void testGetTile() throws IOException, InvalidRangeException {
        int sizeT = 2;
        int sizeC = 3;
        int sizeZ = 4;
        int sizeY = 5;
        int sizeX = 6;
        int resolutions = 1;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "int32", resolutions);
        ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
        int[] data = new int[2*3*4*5*6];
        for (int i = 0; i < 2*3*4*5*6; i++) {
            data[i] = i;
        }
        Tracing.newBuilder().spanReporter(Reporter.NOOP).build();
        test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 2, 2);
            ByteBuffer bb = pixelData.getData();
            bb.order(ByteOrder.BIG_ENDIAN);
            IntBuffer ib = bb.asIntBuffer();
            Assert.assertEquals(ib.get(0), 0);
            Assert.assertEquals(ib.get(1), 1);
            Assert.assertEquals(ib.get(2), 6);
            Assert.assertEquals(ib.get(3), 7);
            pixelData = zpbuf.getTile(1, 1, 1, 1, 1, 2, 2);
            bb = pixelData.getData();
            bb.order(ByteOrder.BIG_ENDIAN);
            ib = bb.asIntBuffer();
            Assert.assertEquals(ib.get(0), 517);//360(6*5*4*3) + 120(6*5*4) + 30(6*5) + 6 + 1
            Assert.assertEquals(ib.get(1), 518);
            Assert.assertEquals(ib.get(2), 523);
            Assert.assertEquals(ib.get(3), 524);
        }
    }

    @Test(expected = DimensionsOutOfBoundsException.class)
    public void testGetTileLargerThanImage()
            throws IOException, InvalidRangeException {
        int sizeT = 2;
        int sizeC = 3;
        int sizeZ = 4;
        int sizeY = 5;
        int sizeX = 6;
        int resolutions = 1;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "int32",
                resolutions);
        ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
        int[] data = new int[2*3*4*5*6];
        for (int i = 0; i < 2*3*4*5*6; i++) {
            data[i] = i;
        }
        Tracing.newBuilder().spanReporter(Reporter.NOOP).build();
        test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            zpbuf.setResolutionLevel(0);
            PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 10, 10);
            ByteBuffer bb = pixelData.getData();
            bb.order(ByteOrder.BIG_ENDIAN);
            IntBuffer ib = bb.asIntBuffer();
            Assert.assertEquals(ib.get(0), 0);
            Assert.assertEquals(ib.get(1), 1);
            Assert.assertEquals(ib.get(2), 6);
            Assert.assertEquals(ib.get(3), 7);
        }
    }

    @Test
    public void testTileExceedsMax() throws IOException, InvalidRangeException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16",
                resolutions);

        Tracing.newBuilder().spanReporter(Reporter.NOOP).build();
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 32)) {
            PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 1, 33);
            Assert.assertNull(pixelData);
        }
    }

    @Test
    public void testCheckBoundsValidZeros() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            zpbuf.checkBounds(0, 0, 0, 0, 0);
        }
    }

    @Test
    public void testCheckBoundsValidEnd() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            zpbuf.checkBounds(2047, 511, 2, 1, 0);
        }
    }

    @Test(expected = DimensionsOutOfBoundsException.class)
    public void testCheckBoundsOutOfRange() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            zpbuf.checkBounds(2048, 511, 2, 1, 0);
        }
    }

    @Test(expected = DimensionsOutOfBoundsException.class)
    public void testCheckBounds() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            zpbuf.checkBounds(-1, 0, 0, 0, 0);
        }
    }

    public void testGetTileSize() throws IOException {
        int sizeT = 1;
        int sizeC = 1;
        int sizeZ = 1;
        int sizeY = 2048;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                new ZarrPixelBuffer(pixels, output.resolve("0"), 1024)) {
            Dimension tileSize = zpbuf.getTileSize();
            Assert.assertEquals(1024, tileSize.getWidth(), 0.1);
            Assert.assertEquals(1024, tileSize.getHeight(), 0.1);
        }
    }

}
