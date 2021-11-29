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
import ome.io.nio.RomioPixelBuffer;
import ome.model.core.Pixels;
import ome.util.PixelData;
import ucar.ma2.InvalidRangeException;

public class ZarrPixelBuffer implements PixelBuffer {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ZarrPixelBuffer.class);

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
     * Get Bio-Formats/OMERO pixels type for buffer.
     * @return See above.
     */
    public int getPixelsType() {
        DataType dataType = array.getDataType();
        switch (dataType) {
            case u1:
                return FormatTools.UINT8;
            case i1:
                return FormatTools.INT8;
            case u2:
                return FormatTools.UINT16;
            case i2:
                return FormatTools.INT16;
            case u4:
                return FormatTools.UINT32;
            case i4:
                return FormatTools.INT32;
            case f4:
                return FormatTools.FLOAT;
            case f8:
                return FormatTools.DOUBLE;
            default:
                throw new IllegalArgumentException(
                        "Data type " + dataType + " not supported");
        }
    }

    /**
     * Calculates the pixel length of a given NumPy like "shape".
     * @param shape the NumPy like "shape" to calculate the length of
     * @return See above
     * @see <a href=
     * "https://numpy.org/doc/stable/reference/generated/numpy.shape.html">
     * numpy.shape</a> documentation
     */
    private int length(int[] shape) {
        return IntStream.of(shape).reduce(1, Math::multiplyExact);
    }

    private void read(byte[] buffer, int[] shape, int[] offset)
            throws IOException {
        if (shape[4] > maxTileLength) {
            throw new IllegalArgumentException(String.format(
                    "width %d > maxTileLength %d", shape[4], maxTileLength));
        }
        if (shape[3] > maxTileLength) {
            throw new IllegalArgumentException(String.format(
                    "height %d > maxTileLength %d", shape[3], maxTileLength));
        }
        if (shape[4] < 0) {
            throw new IllegalArgumentException("width < 0");
        }
        if (shape[3] < 0) {
            throw new IllegalArgumentException("height < 0");
        }
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_bytes");
        try {
            span.tag("omero.zarr.shape", Arrays.toString(shape));
            span.tag("omero.zarr.offset", Arrays.toString(offset));
            span.tag("omero.zarr.array", array.toString());

            ByteBuffer asByteBuffer = ByteBuffer.wrap(buffer);
            DataType dataType = array.getDataType();
            switch (dataType) {
                case u1:
                case i1:
                    array.read(buffer, shape, offset);
                    break;
                case u2:
                case i2:
                {
                    short[] data = (short[]) array.read(shape, offset);
                    asByteBuffer.asShortBuffer().put(data);
                    break;
                }
                case u4:
                case i4:
                {
                    int[] data = (int[]) array.read(shape, offset);
                    asByteBuffer.asIntBuffer().put(data);
                    break;
                }
                case i8:
                {
                    long[] data = (long[]) array.read(shape, offset);
                    asByteBuffer.asLongBuffer().put(data);
                    break;
                }
                case f4:
                {
                    float[] data = (float[]) array.read(shape, offset);
                    asByteBuffer.asFloatBuffer().put(data);
                    break;
                }
                case f8:
                {
                    double[] data = (double[]) array.read(shape, offset);
                    asByteBuffer.asDoubleBuffer().put(data);
                    break;
                }
                default:
                    throw new IllegalArgumentException(
                            "Data type " + dataType + " not supported");
            }
        } catch (InvalidRangeException e) {
            log.error("Error reading Zarr data", e);
            span.error(e);
            throw new IOException(e);
        } catch (Exception e) {
            log.error("Error reading Zarr data", e);
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    private PixelData toPixelData(byte[] buffer) {
        if (buffer == null) {
            return null;
        }
        PixelData d = new PixelData(
                FormatTools.getPixelTypeString(getPixelsType()),
                ByteBuffer.wrap(buffer));
        d.setOrder(ByteOrder.BIG_ENDIAN);
        return d;
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
            chunks.add(shape);
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

    /**
     * No-op.
     */
    @Override
    public void close() throws IOException {
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
        return ((long) getRowSize()) * ((long) getSizeY());
    }

    @Override
    public Integer getRowSize() {
        return getSizeX() * getByteWidth();
    }

    @Override
    public Integer getColSize() {
        return getSizeY() * getByteWidth();
    }

    @Override
    public Long getStackSize() {
        return getPlaneSize() * ((long) getSizeZ());
    }

    @Override
    public Long getTimepointSize() {
        return getStackSize() * ((long) getSizeC());
    }

    @Override
    public Long getTotalSize() {
        return getTimepointSize() * ((long) getSizeT());
    }

    @Override
    public Long getHypercubeSize(List<Integer> offset, List<Integer> size, List<Integer> step)
            throws DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support Hypercube access");
    }

    @Override
    public Long getRowOffset(Integer y, Integer z, Integer c, Integer t)
            throws DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not provide row offsets");
    }

    @Override
    public Long getPlaneOffset(Integer z, Integer c, Integer t)
            throws DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not provide plane offsets");
    }

    @Override
    public Long getStackOffset(Integer c, Integer t)
            throws DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not provide stack offsets");
    }

    @Override
    public Long getTimepointOffset(Integer t)
            throws DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not provide timepoint offsets");
    }

    @Override
    public PixelData getHypercube(
            List<Integer> offset, List<Integer> size, List<Integer> step)
                    throws IOException, DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support Hypercube access");
    }

    @Override
    public byte[] getHypercubeDirect(
            List<Integer> offset, List<Integer> size, List<Integer> step,
            byte[] buffer)
                    throws IOException, DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support Hypercube access");
    }

    @Override
    public byte[] getPlaneRegionDirect(
            Integer z, Integer c, Integer t, Integer count, Integer offset,
            byte[] buffer)
                    throws IOException, DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support plane region access");
    }

    @Override
    public PixelData getTile(
            Integer z, Integer c, Integer t, Integer x, Integer y,
            Integer w, Integer h)
                    throws IOException {
        //Check origin indices > 0
        checkBounds(x, y, z, c, t);
        //Check check bottom-right of tile in bounds
        checkBounds(x + w - 1, y + h - 1, z, c, t);
        int[] shape = new int[] { 1, 1, 1, h, w };
        byte[] buffer = new byte[length(shape) * getByteWidth()];
        return toPixelData(getTileDirect(z, c, t, x, y, w, h, buffer));
    }

    @Override
    public byte[] getTileDirect(
            Integer z, Integer c, Integer t, Integer x, Integer y,
            Integer w, Integer h, byte[] buffer) throws IOException {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_tile_direct");
        try {
            //Check origin indices > 0
            checkBounds(x, y, z, c, t);
            //Check check bottom-right of tile in bounds
            checkBounds(x + w - 1, y + h - 1, z, c, t);
            int[] shape = new int[] { 1, 1, 1, h, w };
            int[] offset = new int[] { t, c, z, y, x };
            read(buffer, shape, offset);
            return buffer;
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
    public PixelData getRegion(Integer size, Long offset) throws IOException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support region access");
    }

    @Override
    public byte[] getRegionDirect(Integer size, Long offset, byte[] buffer)
            throws IOException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support region access");
    }

    @Override
    public PixelData getRow(Integer y, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        return toPixelData(getRowDirect(y, z, c, t, new byte[getRowSize()]));
    }

    @Override
    public PixelData getCol(Integer x, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        return toPixelData(getColDirect(x, z, c, t, new byte[getColSize()]));
    }

    @Override
    public byte[] getRowDirect(
            Integer y, Integer z, Integer c, Integer t, byte[] buffer)
                    throws IOException, DimensionsOutOfBoundsException {
        int x = 0;
        int w = getSizeX();
        int h = 1;
        return getTileDirect(z, c, t, x, y, w, h, buffer);
    }

    @Override
    public byte[] getColDirect(
            Integer x, Integer z, Integer c, Integer t, byte[] buffer)
                    throws IOException, DimensionsOutOfBoundsException {
        int y = 0;
        int w = 1;
        int h = getSizeY();
        return getTileDirect(z, c, t, x, y, w, h, buffer);
    }

    @Override
    public PixelData getPlane(Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        int planeSize = RomioPixelBuffer.safeLongToInteger(getPlaneSize());
        return toPixelData(getPlaneDirect(z, c, t, new byte[planeSize]));
    }

    @Override
    public PixelData getPlaneRegion(Integer x, Integer y, Integer width,
            Integer height, Integer z, Integer c, Integer t, Integer stride)
                    throws IOException, DimensionsOutOfBoundsException {
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support plane region access");
    }

    @Override
    public byte[] getPlaneDirect(Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        int y = 0;
        int x = 0;
        int w = getSizeX();
        int h = getSizeY();
        return getTileDirect(z, c, t, x, y, w, h, buffer);
    }

    @Override
    public PixelData getStack(Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        int stackSize = RomioPixelBuffer.safeLongToInteger(getStackSize());
        byte[] buffer = new byte[stackSize];
        return toPixelData(getStackDirect(c, t, buffer));
    }

    @Override
    public byte[] getStackDirect(Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        int x = 0;
        int y = 0;
        int z = 0;
        int w = getSizeX();
        int h = getSizeY();

        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_stack_direct");
        try {
            //Check origin indices > 0
            checkBounds(x, y, z, c, t);
            //Check check bottom-right of tile in bounds
            checkBounds(x + w - 1, y + h - 1, z, c, t);
            int[] shape = new int[] { 1, 1, getSizeZ(), h, w };
            int[] offset = new int[] { t, c, z, y, x };
            read(buffer, shape, offset);
            return buffer;
        } catch (Exception e) {
            log.error("Error while retrieving pixel data", e);
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

    @Override
    public PixelData getTimepoint(Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        int timepointSize =
                RomioPixelBuffer.safeLongToInteger(getTimepointSize());
        byte[] buffer = new byte[timepointSize];
        return toPixelData(getTimepointDirect(t, buffer));
    }

    @Override
    public byte[] getTimepointDirect(Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        int x = 0;
        int y = 0;
        int z = 0;
        int c = 0;
        int w = getSizeX();
        int h = getSizeY();

        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_timepoint_direct");
        try {
            //Check origin indices > 0
            checkBounds(x, y, z, c, t);
            //Check check bottom-right of tile in bounds
            checkBounds(x + w - 1, y + h - 1, z, c, t);
            int[] shape = new int[] { 1, getSizeC(), getSizeZ(), h, w };
            int[] offset = new int[] { t, c, z, y, x };
            read(buffer, shape, offset);
            return buffer;
        } catch (Exception e) {
            log.error("Error while retrieving pixel data", e);
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
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
        throw new UnsupportedOperationException(
                "Zarr pixel buffer does not support message digest " +
                "calculation");
    }

    @Override
    public int getByteWidth() {
        return FormatTools.getBytesPerPixel(getPixelsType());
    }

    @Override
    public boolean isSigned() {
        return FormatTools.isSigned(getPixelsType());
    }

    @Override
    public boolean isFloat() {
        return FormatTools.isFloatingPoint(getPixelsType());
    }

    @Override
    public String getPath() {
        return root.toString();
    }

    @Override
    public long getId() {
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
        try {
            int[] chunks = getChunks()[resolutionLevel];
            return new Dimension(chunks[4], chunks[3]);
        } catch (Exception e) {
            // FIXME: Throw the right exception
            throw new RuntimeException(e);
        }
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
                resolutionDescriptions.add(Arrays.asList(
                                (int) (sizeX / scale), (int) (sizeY / scale)));
            }
            return resolutionDescriptions;
        } catch (Exception e) {
            // FIXME: Throw the right exception
            throw new RuntimeException(e);
        }
    }

}
