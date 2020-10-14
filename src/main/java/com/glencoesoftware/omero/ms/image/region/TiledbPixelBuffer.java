package com.glencoesoftware.omero.ms.image.region;

import java.awt.Dimension;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.image.region.TiledbUtils;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import loci.formats.FormatTools;
import ome.io.bioformats.BfPixelBuffer;
import ome.io.bioformats.BfPixelsWrapper;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.io.nio.PixelBuffer;
import ome.io.nio.RomioPixelBuffer;
import ome.model.core.Pixels;
import ome.util.PixelData;

public class TiledbPixelBuffer implements PixelBuffer {

    Pixels pixels;

    String ngffDir;

    Long filesetId;

    int resolutionLevel = 0;

    public TiledbPixelBuffer(Pixels pixels, String ngffDir, Long filesetId) {
        this.pixels = pixels;
        this.ngffDir = ngffDir;
        this.filesetId = filesetId;
    }

    private final static Logger log = LoggerFactory.getLogger(TiledbPixelBuffer.class);

String getPixelsType(Datatype type) {
        switch (type) {
            case TILEDB_UINT8:
                return FormatTools.getPixelTypeString(FormatTools.UINT8);
            case TILEDB_INT8:
                return FormatTools.getPixelTypeString(FormatTools.INT8);
            case TILEDB_UINT16:
                return FormatTools.getPixelTypeString(FormatTools.UINT16);
            case TILEDB_INT16:
                return FormatTools.getPixelTypeString(FormatTools.INT16);
            case TILEDB_UINT32:
                return FormatTools.getPixelTypeString(FormatTools.UINT32);
            case TILEDB_INT32:
                return FormatTools.getPixelTypeString(FormatTools.INT32);
            case TILEDB_FLOAT32:
                return FormatTools.getPixelTypeString(FormatTools.FLOAT);
            case TILEDB_FLOAT64:
                return FormatTools.getPixelTypeString(FormatTools.DOUBLE);
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
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
        log.info("getHypercube");
        return null;
    }

    @Override
    public byte[] getHypercubeDirect(List<Integer> offset, List<Integer> size, List<Integer> step, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        log.info("getHypercubeDirect");
        return null;
    }

    @Override
    public byte[] getPlaneRegionDirect(Integer z, Integer c, Integer t, Integer count, Integer offset, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        log.info("getPlaneRegionDirect");
        return null;
    }

    @Override
    public PixelData getTile(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h)
            throws IOException {
        // TODO Auto-generated method stub
        log.info("getTile");
        Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId())
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()) + "/" + Integer.toString(resolutionLevel));
        try (Context ctx = new Context();
                Array array = new Array(ctx, tiledbDataPath.toString(), QueryType.TILEDB_READ)){
            PixelData d;
            byte[] buffer = getData(array, ctx);
            d = new PixelData(getPixelsType(array.getSchema().getAttribute("a1").getType()), ByteBuffer.wrap(buffer));
            log.info("PIXEL DATA BYTES PER PIXEL: " + Integer.toString(d.bytesPerPixel()));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (TileDBError e) {
            log.error("TileDBError when trying to getPlaneDirect", e);
        }
        return null;
    }

    @Override
    public byte[] getTileDirect(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h,
            byte[] buffer) throws IOException {
        // TODO Auto-generated method stub
        log.info("getTileDirect");
        return null;
    }

    @Override
    public PixelData getRegion(Integer size, Long offset) throws IOException {
        // TODO Auto-generated method stub
        log.info("getRegion");
        return null;
    }

    @Override
    public byte[] getRegionDirect(Integer size, Long offset, byte[] buffer) throws IOException {
        // TODO Auto-generated method stub
        log.info("getRegionDirect");
        return null;
    }

    @Override
    public PixelData getRow(Integer y, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        log.info("getRow");
        return null;
    }

    @Override
    public PixelData getCol(Integer x, Integer z, Integer c, Integer t)
            throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        log.info("getCol");
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
        log.info("getPlane");
        return null;
    }

    @Override
    public PixelData getPlaneRegion(Integer x, Integer y, Integer width, Integer height, Integer z, Integer c,
            Integer t, Integer stride) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        log.info("getPlaneRegion");
        return null;
    }

    @Override
    public byte[] getPlaneDirect(Integer z, Integer c, Integer t, byte[] buffer)
            throws IOException, DimensionsOutOfBoundsException {
        Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId())
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()) + "/" + Integer.toString(resolutionLevel));
        try (Context ctx = new Context();
                Array array = new Array(ctx, tiledbDataPath.toString(), QueryType.TILEDB_READ)){
        } catch (TileDBError e) {
            log.error("TileDBError when trying to getPlaneDirect", e);
        }
        return null;
    }

    @Override
    public PixelData getStack(Integer c, Integer t) throws IOException, DimensionsOutOfBoundsException {
        Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId())
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()) + "/" + Integer.toString(resolutionLevel));
        try (Context ctx = new Context();
                Array array = new Array(ctx, tiledbDataPath.toString(), QueryType.TILEDB_READ)){
            PixelData d;
            byte[] buffer = getData(array, ctx);
            d = new PixelData(getPixelsType(array.getSchema().getAttribute("a1").getType()), ByteBuffer.wrap(buffer));
            log.info("PIXEL DATA BYTES PER PIXEL: " + Integer.toString(d.bytesPerPixel()));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (TileDBError e) {
            log.error("TileDBError when trying to getPlaneDirect", e);
        }
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
        log.info("getTimepoint");
        return null;
    }

    @Override
    public byte[] getTimepointDirect(Integer t, byte[] buffer) throws IOException, DimensionsOutOfBoundsException {
        // TODO Auto-generated method stub
        log.info("getTimepointDirect");
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
        return Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId())
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()) + "/" + Integer.toString(resolutionLevel)).toString();
    }

    @Override
    public long getId() {
        // TODO Auto-generated method stub
        return 0;
    }

    private int getDimSize(String dimName) {
        Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId())
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()) + "/" + Integer.toString(resolutionLevel));
        try (Context ctx = new Context();
                Array array = new Array(ctx, tiledbDataPath.toString(), QueryType.TILEDB_READ)){
            Domain dom = array.getSchema().getDomain();
            return (int) ((long) dom.getDimension(dimName).getDomain().getSecond()
                    + (long) dom.getDimension(dimName).getDomain().getFirst() + 1);
        } catch (TileDBError e) {
            log.error("TileDBError in TiledbPixelBuffer", e);
        }
        return 0;
    }

    @Override
    public int getSizeX() {
        return getDimSize("x");
    }

    @Override
    public int getSizeY() {
        return getDimSize("y");
    }

    @Override
    public int getSizeZ() {
        return getDimSize("z");
    }

    @Override
    public int getSizeC() {
        return getDimSize("c");
    }

    @Override
    public int getSizeT() {
        return getDimSize("t");
    }

    @Override
    public int getResolutionLevels() {
        Path tiledbSeriesPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()));
        File[] directories = new File(tiledbSeriesPath.toString()).listFiles(File::isDirectory);
        int count = 0;
        for(File dir : directories) {
            try {
                Integer.valueOf(dir.getName());
                count++;
            } catch(NumberFormatException e) {
            }
        }
        return count;
    }

    @Override
    public int getResolutionLevel() {
        return resolutionLevel;
    }

    @Override
    public void setResolutionLevel(int resolutionLevel) {
        this.resolutionLevel = resolutionLevel;
    }

    @Override
    public Dimension getTileSize() {
        // TODO Auto-generated method stub
        log.info("getTileSize");
        return new Dimension(getSizeX(), getSizeY());
    }

    @Override
    public List<List<Integer>> getResolutionDescriptions() {
        // TODO Auto-generated method stub
        return null;
    }


    private byte[] getData(Array array, Context ctx) throws TileDBError {
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();
        Attribute attribute = schema.getAttribute("a1");

        int bytesPerPixel = TiledbUtils.getBytesPerPixel(attribute.getType());

        int num_dims = (int) domain.getNDim();
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }
        long[] subarrayDomain = new long[5*2];

        subarrayDomain[6] = (long) domain.getDimension("y").getDomain().getFirst();
        subarrayDomain[7] = (long) domain.getDimension("y").getDomain().getSecond();
        subarrayDomain[8] = (long) domain.getDimension("x").getDomain().getFirst();
        subarrayDomain[9] = (long) domain.getDimension("x").getDomain().getSecond();
        log.info(Arrays.toString(subarrayDomain));

        int capacity = ((int) (subarrayDomain[7] - subarrayDomain[6] + 1))
                        * ((int) (subarrayDomain[9] - subarrayDomain[8] + 1))
                        * bytesPerPixel;
        log.info(Integer.toString(capacity));

        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
        buffer.order(ByteOrder.nativeOrder());
        //Dimensions in Dense Arrays must be the same type
        try (Query query = new Query(array, QueryType.TILEDB_READ);
                NativeArray subArray = new NativeArray(ctx, subarrayDomain, Datatype.TILEDB_INT64)){
            query.setSubarray(subArray);
            query.setBuffer("a1", buffer);
            query.submit();
            byte[] outputBytes = new byte[buffer.capacity()];
            buffer.get(outputBytes);
            return outputBytes;
        }
    }

}