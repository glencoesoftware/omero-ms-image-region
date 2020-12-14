package com.glencoesoftware.omero.ms.image.region;

import java.awt.Dimension;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.image.region.TiledbUtils;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.io.nio.PixelBuffer;
import ome.model.core.Pixels;
import ome.util.PixelData;

public class TiledbPixelBuffer implements PixelBuffer {

    Pixels pixels;

    String ngffDir;

    Long filesetId;

    int resolutionLevel;

    TiledbUtils tiledbUtils;

    public TiledbPixelBuffer(Pixels pixels, String ngffDir, Long filesetId, TiledbUtils tiledbUtils) {
        this.pixels = pixels;
        this.ngffDir = ngffDir;
        this.filesetId = filesetId;
        this.tiledbUtils = tiledbUtils;
        this.resolutionLevel = this.getResolutionLevels() - 1;
        if (this.resolutionLevel < 0) {
            throw new IllegalArgumentException("This TileDB file has no pixel data");
        }
    }

    private final static Logger log = LoggerFactory.getLogger(TiledbPixelBuffer.class);

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
        StringBuffer domStrBuf = new StringBuffer();
        domStrBuf.append("[")
            .append(t).append(",")
            .append(c).append(",")
            .append(z).append(",")
            .append(y).append(":").append(y + h).append(",")
            .append(x).append(":").append(x + w).append("]");
        try {
            return tiledbUtils.getPixelData(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel,
                    domStrBuf.toString());
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
        return null;
    }

    @Override
    public PixelData getStack(Integer c, Integer t) throws IOException, DimensionsOutOfBoundsException {
        Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId())
                + ".tiledb/" + Integer.toString(pixels.getImage().getSeries()) + "/" + Integer.toString(resolutionLevel));
        try (Context ctx = new Context();
                Array array = new Array(ctx, tiledbDataPath.toString(), QueryType.TILEDB_READ)){
            PixelData d;
            byte[] buffer = TiledbUtils.getData(array, ctx);
            d = new PixelData(TiledbUtils.getPixelsType(array.getSchema().getAttribute("a1").getType()), ByteBuffer.wrap(buffer));
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
        return Paths.get(ngffDir).resolve(Long.toString(pixels.getImage().getFileset().getId()) + ".tiledb")
                .resolve(Integer.toString(pixels.getImage().getSeries()))
                .resolve(Integer.toString(resolutionLevel)).toString();
    }

    @Override
    public long getId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getSizeX() {
        return tiledbUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, "x");
    }

    @Override
    public int getSizeY() {
        return tiledbUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, "y");
    }

    @Override
    public int getSizeZ() {
        return tiledbUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, "z");
    }

    @Override
    public int getSizeC() {
        return tiledbUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, "c");
    }

    @Override
    public int getSizeT() {
        return tiledbUtils.getDimSize(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel, "t");
    }

    @Override
    public int getResolutionLevels() {
        return tiledbUtils.getResolutionLevels(ngffDir, filesetId, pixels.getImage().getSeries());
    }

    @Override
    public int getResolutionLevel() {
        return Math.abs(
                resolutionLevel - (getResolutionLevels() - 1));
    }

    @Override
    public void setResolutionLevel(int resolutionLevel) {
        this.resolutionLevel = Math.abs(
                resolutionLevel - (getResolutionLevels() - 1));
    }

    @Override
    public Dimension getTileSize() {
        return new Dimension(getSizeX(), getSizeY());
    }

    @Override
    public List<List<Integer>> getResolutionDescriptions() {
        int resLevels = getResolutionLevels();
        List<List<Integer>> resolutionDescriptions = new ArrayList<List<Integer>>();
        int originalResolution = resolutionLevel;
        for(int i = 0; i < resLevels; i++) {
            this.resolutionLevel = i;
            List<Integer> description = new ArrayList<Integer>();
            Integer[] xy = tiledbUtils.getSizeXandY(ngffDir, filesetId, pixels.getImage().getSeries(), resolutionLevel);
            description.add(xy[0]);
            description.add(xy[1]);
            resolutionDescriptions.add(description);
        }
        setResolutionLevel(originalResolution);
        return resolutionDescriptions;
    }
}