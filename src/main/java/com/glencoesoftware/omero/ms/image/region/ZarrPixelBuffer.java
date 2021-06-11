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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.slf4j.LoggerFactory;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;

import brave.ScopedSpan;
import brave.Tracing;
import loci.formats.FormatTools;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.io.nio.PixelBuffer;
import ome.model.core.Pixels;
import ome.util.PixelData;
import ucar.ma2.InvalidRangeException;

public class ZarrPixelBuffer implements PixelBuffer {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(PixelBuffer.class);

    /** Reference to the pixels. */
    private final Pixels pixels;

    /** Root of the OME-NGFF multiscale we are operating on */
    private final Path root;

    /** Requested resolution level */
    private int resolutionLevel;

    /** Total number of resolution levels */
    private final int resolutionLevels;

    /** Max tile length */
    private final Integer maxTileLength;

    /** Root group of the OME-NGFF multiscale we are operating on */
    private final ZarrGroup rootGroup;

    /** Zarr attributes present on the root group */
    private final Map<String, Object> rootGroupAttributes;

    /** Zarr array corresponding to the current resolution level */
    private ZarrArray array;

    /**
     * Default constructor
     * @param root The root of this buffer
     * @param maxTileLength Maximum tile length that can be used during
     * read operations
     * @throws IOException
     */
    public ZarrPixelBuffer(Pixels pixels, Path root, Integer maxTileLength)
            throws IOException {
        this.pixels = pixels;
        this.root = root;
        rootGroup = ZarrGroup.open(this.root);
        rootGroupAttributes = rootGroup.getAttributes();
        if (!rootGroupAttributes.containsKey("multiscales")) {
            throw new IllegalArgumentException("Missing multiscales metadata!");
        }
        this.resolutionLevels = this.getResolutionLevels();
        setResolutionLevel(this.resolutionLevels - 1);
        if (this.resolutionLevel < 0) {
            throw new IllegalArgumentException(
                    "This Zarr file has no pixel data");
        }
        this.maxTileLength = maxTileLength;
    }

    /**
     * Get Bio-Formats/OMERO pixels type string for buffer.
     * @return See above.
     */
    public String getPixelsType() {
        DataType dataType = array.getDataType();
        switch (dataType) {
            case u1:
                return FormatTools.getPixelTypeString(FormatTools.UINT8);
            case i1:
                return FormatTools.getPixelTypeString(FormatTools.INT8);
            case u2:
                return FormatTools.getPixelTypeString(FormatTools.UINT16);
            case i2:
                return FormatTools.getPixelTypeString(FormatTools.INT16);
            case u4:
                return FormatTools.getPixelTypeString(FormatTools.UINT32);
            case i4:
                return FormatTools.getPixelTypeString(FormatTools.INT32);
            case f4:
                return FormatTools.getPixelTypeString(FormatTools.FLOAT);
            case f8:
                return FormatTools.getPixelTypeString(FormatTools.DOUBLE);
            default:
                throw new IllegalArgumentException(
                        "Data type " + dataType + " not supported");
        }
    }

    /**
     * Get bytes per pixel for the current buffer data type
     * @return See above.
     */
    private int getBytesPerPixel() {
        DataType dataType = array.getDataType();
        switch (dataType) {
            case u1:
            case i1:
                return 1;
            case u2:
            case i2:
                return 2;
            case u4:
            case i4:
            case f4:
                return 4;
            case i8:
            case f8:
                return 8;
            default:
                throw new IllegalArgumentException(
                        "Data type " + dataType + " not supported");
        }
    }

    /**
     * Get byte array from ZarrArray
     * @param zarray The ZarrArray to get data from
     * @param shape The shape of the region to retrieve
     * @param offset The offset of the region
     * @return byte array of data from the ZarrArray
     */
    private ByteBuffer getBytes(int[] shape, int[] offset) {
        if (shape[4] > maxTileLength) {
            throw new IllegalArgumentException(String.format(
                    "sizeX %d > maxTileLength %d", shape[4], maxTileLength));
        }
        if (shape[3] > maxTileLength) {
            throw new IllegalArgumentException(String.format(
                    "sizeY %d > maxTileLength %d", shape[3], maxTileLength));
        }
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_bytes");
        try {
            span.tag("omero.zarr.shape", Arrays.toString(shape));
            span.tag("omero.zarr.offset", Arrays.toString(offset));
            span.tag("omero.zarr.array", array.toString());
            int length = IntStream.of(shape).reduce(1, Math::multiplyExact);
            int bytesPerPixel = getBytesPerPixel();
            ByteBuffer asByteBuffer = ByteBuffer.allocate(
                    length * bytesPerPixel);
            DataType dataType = array.getDataType();
            switch (dataType) {
                case u1:
                case i1:
                    return ByteBuffer.wrap((byte[]) array.read(shape, offset));
                case u2:
                case i2:
                {
                    short[] data = (short[]) array.read(shape, offset);
                    asByteBuffer.asShortBuffer().put(data);
                    return asByteBuffer;
                }
                case u4:
                case i4:
                {
                    int[] data = (int[]) array.read(shape, offset);
                    asByteBuffer.asIntBuffer().put(data);
                    return asByteBuffer;
                }
                case i8:
                {
                    long[] data = (long[]) array.read(shape, offset);
                    asByteBuffer.asLongBuffer().put(data);
                    return asByteBuffer;
                }
                case f4:
                {
                    float[] data = (float[]) array.read(shape, offset);
                    asByteBuffer.asFloatBuffer().put(data);
                    return asByteBuffer;
                }
                case f8:
                {
                    double[] data = (double[]) array.read(shape, offset);
                    asByteBuffer.asDoubleBuffer().put(data);
                    return asByteBuffer;
                }
                default:
                    log.error("Unsupported data type" + dataType);
                    return null;
            }
        } catch (InvalidRangeException|IOException e) {
            log.error("Error getting zarr PixelData", e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Retrieves the array chunk sizes of all subresolutions of this multiscale
     * buffer.
     * @return See above.
     * @throws IOException
     */
    public int[][] getChunks() throws IOException {
        List<Map<String, String>> datasets = getDatasets();
        List<int[]> chunks = new ArrayList<int[]>();
        for (Map<String, String> dataset : datasets) {
            ZarrArray resolutionArray = ZarrArray.open(
                    root.resolve(dataset.get("path")));
            int[] shape = resolutionArray.getChunks();
            chunks.add(0, shape);
        }
        return chunks.toArray(new int[chunks.size()][]);
    }

    /**
     * Retrieves the datasets metadat of the first multiscale from the root
     * group attributes.
     * @return See above.
     * @see #getRootGroupAttributes()
     * @see #getMultiscalesMetadata()
     */
    public List<Map<String, String>> getDatasets() {
        return (List<Map<String, String>>)
                getMultiscalesMetadata().get(0).get("datasets");
    }

    /**
     * Retrieves the multiscales metadata from the root group attributes.
     * @return See above.
     * @see #getRootGroupAttributes()
     * @see #getDatasets()
     */
    public List<Map<String, Object>> getMultiscalesMetadata() {
        return (List<Map<String, Object>>)
                rootGroupAttributes.get("multiscales");
    }

    /**
     * Returns the current Zarr root group attributes for this buffer.
     * @return See above.
     * @see #getMultiscalesMetadata()
     * @see #getDatasets()
     */
    public Map<String, Object> getRootGroupAttributes() {
        return rootGroupAttributes;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    /**
     * Implemented as specified by {@link PixelBuffer} I/F.
     * @see PixelBuffer#checkBounds(Integer, Integer, Integer, Integer, Integer)
     */
    public void checkBounds(Integer x, Integer y, Integer z, Integer c,
            Integer t)
            throws DimensionsOutOfBoundsException {
        if (x != null && (x > getSizeX() - 1 || x < 0)) {
            throw new DimensionsOutOfBoundsException("X '" + x
                    + "' greater than sizeX '" + getSizeX() + "'.");
        }
        if (y != null && (y > getSizeY() - 1 || y < 0)) {
            throw new DimensionsOutOfBoundsException("Y '" + y
                    + "' greater than sizeY '" + getSizeY() + "'.");
        }

        if (z != null && (z > getSizeZ() - 1 || z < 0)) {
            throw new DimensionsOutOfBoundsException("Z '" + z
                    + "' greater than sizeZ '" + getSizeZ() + "'.");
        }

        if (c != null && (c > getSizeC() - 1 || c < 0)) {
            throw new DimensionsOutOfBoundsException("C '" + c
                    + "' greater than sizeC '" + getSizeC() + "'.");
        }

        if (t != null && (t > getSizeT() - 1 || t < 0)) {
            throw new DimensionsOutOfBoundsException("T '" + t
                    + "' greater than sizeT '" + getSizeT() + "'.");
        }
    }

    @Override
    public Long getPlaneSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer getRowSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Integer getColSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getStackSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getTimepointSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getTotalSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getHypercubeSize(List<Integer> offset, List<Integer> size, List<Integer> step)
            throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getRowOffset(Integer y, Integer z, Integer c, Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getPlaneOffset(Integer z, Integer c, Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getStackOffset(Integer c, Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long getTimepointOffset(Integer t) throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getHypercube(List<Integer> offset, List<Integer> size, List<Integer> step)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getHypercubeDirect(List<Integer> offset, List<Integer> size, List<Integer> step, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getPlaneRegionDirect(Integer z, Integer c, Integer t, Integer count, Integer offset, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getTile(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h)
            throws IOException {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixel_data_from_zarr");
        try {
            //Check origin indices > 0
            checkBounds(x, y, z, c, t);
            //Check check bottom-right of tile in bounds
            checkBounds(x + w - 1, y + h - 1, z, c, t);
            int[] shape = new int[] { 1, 1, 1, h, w };
            int[] offset = new int[] { t, c, z, y, x };
            PixelData d = new PixelData(
                    getPixelsType(), getBytes(shape, offset));
            d.setOrder(ByteOrder.BIG_ENDIAN);
            return d;
        } catch (DimensionsOutOfBoundsException e) {
            log.error("Tile dimension error while retrieving pixel data", e);
            span.error(e);
            throw(e);
        } catch (Exception e) {
            log.error("Error while retrieving pixel data", e);
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

    @Override
    public byte[] getTileDirect(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h,
            byte[] buffer) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getRegion(Integer size, Long offset) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getRegionDirect(Integer size, Long offset, byte[] buffer) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getRow(Integer y, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getCol(Integer x, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getRowDirect(Integer y, Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getColDirect(Integer x, Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getPlane(Integer z, Integer c, Integer t) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getPlaneRegion(Integer x, Integer y, Integer width, Integer height, Integer z, Integer c,
            Integer t, Integer stride) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getPlaneDirect(Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getStack(Integer c, Integer t) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getStackDirect(Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PixelData getTimepoint(Integer t) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] getTimepointDirect(Integer t, byte[] buffer) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTile(byte[] buffer, Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h)
            throws IOException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRegion(Integer size, Long offset, byte[] buffer) throws IOException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRegion(Integer size, Long offset, ByteBuffer buffer) throws IOException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRow(ByteBuffer buffer, Integer y, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPlane(ByteBuffer buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPlane(byte[] buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStack(ByteBuffer buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStack(byte[] buffer, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimepoint(ByteBuffer buffer, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimepoint(byte[] buffer, Integer t)
            throws IOException, DimensionsOutOfBoundsException, BufferOverflowException {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] calculateMessageDigest() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getByteWidth() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isSigned() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isFloat() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getPath() {
        return root.toString();
    }

    @Override
    public long getId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getSizeX() {
        return array.getShape()[4];
    }

    @Override
    public int getSizeY() {
        return array.getShape()[3];
    }

    @Override
    public int getSizeZ() {
        return array.getShape()[2];
    }

    @Override
    public int getSizeC() {
        return array.getShape()[1];
    }

    @Override
    public int getSizeT() {
        return array.getShape()[0];
    }

    @Override
    public int getResolutionLevels() {
        return getDatasets().size();
    }

    @Override
    public int getResolutionLevel() {
        return Math.abs(
                resolutionLevel - (resolutionLevels - 1));
    }

    @Override
    public void setResolutionLevel(int resolutionLevel) {
        this.resolutionLevel = Math.abs(
                resolutionLevel - (resolutionLevels - 1));
        if (this.resolutionLevel < 0) {
            throw new IllegalArgumentException(
                    "This Zarr file has no pixel data");
        }
        try {
            array = ZarrArray.open(
                    root.resolve(Integer.toString(this.resolutionLevel)));
        } catch (Exception e) {
            // FIXME: Throw the right exception
            throw new RuntimeException(e);
        }
    }

    @Override
    public Dimension getTileSize() {
        return new Dimension(getSizeX(), getSizeY());
    }

    @Override
    public List<List<Integer>> getResolutionDescriptions() {
        try {
            int resolutionLevels = getResolutionLevels();
            List<List<Integer>> resolutionDescriptions =
                    new ArrayList<List<Integer>>();
            int sizeX = pixels.getSizeX();
            int sizeY = pixels.getSizeY();
            for (int i = 0; i < resolutionLevels; i++) {
                double scale = Math.pow(2, i);
                resolutionDescriptions.add(
                        0, Arrays.asList(
                                (int) (sizeX / scale), (int) (sizeY / scale)));
            }
            return resolutionDescriptions;
        } catch (Exception e) {
            // FIXME: Throw the right exception
            throw new RuntimeException(e);
        }
    }

}
