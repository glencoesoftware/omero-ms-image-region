package com.glencoesoftware.omero.ms.image.region;

import java.awt.Dimension;
import java.io.IOException;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
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

    /** The Pixels object represented by this PixelBuffer */
    private final Pixels pixels;

    /** Root of the OME-NGFF multiscale we are operating on */
    private final URI root;

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
     * @param pixels The Pixels object represented by this PixelBuffer
     * @param ngffDir Top-level location for NGFF files
     * @param filesetId Fileset ID
     * @param zarrUtils For performing zarr operations
     * @throws IOException 
     */
    public ZarrPixelBuffer(Pixels pixels, URI root, Integer maxTileLength)
            throws IOException {
        this.pixels = pixels;
        this.root = root;
        if ("s3".equals(root.getScheme())) {
            // FIXME: Do something special?
        }
        rootGroup = ZarrGroup.open(Paths.get(root));
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
     * Get byte array from ZarrArray
     * @param zarray The ZarrArray to get data from
     * @param shape The shape of the region to retrieve
     * @param offset The offset of the region
     * @return byte array of data from the ZarrArray
     */
    private byte[] getBytes(int[] shape, int[] offset) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_bytes_zarr");
        if (shape[4] > maxTileLength) {
            throw new IllegalArgumentException(String.format(
                    "sizeX %d > maxTileLength %d", shape[4], maxTileLength));
        }
        if (shape[3] > maxTileLength) {
            throw new IllegalArgumentException(String.format(
                    "sizeX %d > maxTileLength %d", shape[3], maxTileLength));
        }
        try {
            DataType type = array.getDataType();
            int length = IntStream.of(shape).sum();
            int bytesPerPixel = FormatTools.getBytesPerPixel(
                    pixels.getPixelsType().getValue());
            ByteBuffer asByteBuffer = ByteBuffer.allocate(
                    length * bytesPerPixel);
            switch (type) {
                case u1:
                case i1:
                    return (byte[]) array.read(shape, offset);
                case u2:
                case i2:
                {
                    short[] data = (short[]) array.read(shape, offset);
                    asByteBuffer.asShortBuffer().put(data);
                    return asByteBuffer.array();
                }
                case u4:
                case i4:
                {
                    int[] data = (int[]) array.read(shape, offset);
                    asByteBuffer.asIntBuffer().put(data);
                    return asByteBuffer.array();
                }
                case i8:
                {
                    long[] data = (long[]) array.read(shape, offset);
                    asByteBuffer.asLongBuffer().put(data);
                    return asByteBuffer.array();
                }
                case f4:
                {
                    float[] data = (float[]) array.read(shape, offset);
                    asByteBuffer.asFloatBuffer().put(data);
                    return asByteBuffer.array();
                }
                case f8:
                {
                    double[] data = (double[]) array.read(shape, offset);
                    asByteBuffer.asDoubleBuffer().put(data);
                    return asByteBuffer.array();
                }
                default:
                    log.error("Unsupported data type" + type.toString());
                    return null;
            }
        } catch (InvalidRangeException|IOException e) {
            log.error("Error getting zarr PixelData", e);
            return null;
        } finally {
            span.finish();
        }
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkBounds(Integer x, Integer y, Integer z, Integer c, Integer t)
            throws DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub

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
            int[] shape = new int[] { 1, 1, 1, h, w };
            int[] offsets = new int[] { t, c, z, y, x };
            byte[] asArray = getBytes(shape, offsets);
            PixelData d = new PixelData(
                    pixels.getPixelsType().getValue(), ByteBuffer.wrap(asArray));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
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
            List<Map<String, Object>> multiscales =
                    (List<Map<String, Object>>)
                            rootGroupAttributes.get("multiscales");
            return ((List<Map<String, String>>)
                            multiscales.get(0).get("datasets")).size();
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
        String subPath = String.format("/%d", this.resolutionLevel);
        URI path = root.resolve(root.getPath() + subPath);
        if ("s3".equals(root.getScheme())) {
            // FIXME: Do something special?
        }
        try {
            array = ZarrArray.open(Paths.get(path));
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
        List<Map<String, Object>> multiscales =
                (List<Map<String, Object>>)
                        rootGroupAttributes.get("multiscales");
        List<Map<String, String>> datasets =
                (List<Map<String, String>>)
                        multiscales.get(0).get("datasets");
        List<List<Integer>> resolutionDescriptions =
                new ArrayList<List<Integer>>();
        for (Map<String, String> dataset : datasets) {
            if ("s3".equals(root.getScheme())) {
                // FIXME: Do something special?
            }
            String subPath = String.format("/%s", dataset.get("path"));
            URI path = root.resolve(root.getPath() + subPath);
            try {
                ZarrArray resolutionArray = ZarrArray.open(Paths.get(path));
                int[] shape = resolutionArray.getShape();
                resolutionDescriptions.add(
                        0, Arrays.asList(shape[4], shape[3]));
            } catch (Exception e) {
                // FIXME: Throw the right exception
                throw new RuntimeException(e);
            }
        }
        return resolutionDescriptions;
    }

}
