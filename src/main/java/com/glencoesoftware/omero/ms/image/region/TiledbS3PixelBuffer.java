package com.glencoesoftware.omero.ms.image.region;

import java.awt.Dimension;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tiledb.java.api.Array;
import io.tiledb.java.api.Config;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.io.nio.PixelBuffer;
import ome.model.core.Pixels;
import ome.util.PixelData;

public class TiledbS3PixelBuffer implements PixelBuffer {

    Pixels pixels;

    String ngffDir;

    Long filesetId;

    int resolutionLevel;

    /** AWS Access Key */
    private String accessKey;

    /** AWS Secret Key */
    private String secretKey;

    /** AWS Region */
    private String awsRegion;

    /** AWS S3 Endpoint Override */
    private String s3EndpointOverride;

    private final static Logger log = LoggerFactory.getLogger(TiledbS3PixelBuffer.class);

    public TiledbS3PixelBuffer(Pixels pixels, String ngffDir, Long filesetId,
            String accessKey, String secretKey, String awsRegion, String s3EndpointOverride) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.awsRegion = awsRegion;
        this.s3EndpointOverride = s3EndpointOverride;
        this.pixels = pixels;
        this.ngffDir = ngffDir;
        this.filesetId = filesetId;
        this.resolutionLevel = this.getResolutionLevels() - 1;
    }

    @Override
    public int getResolutionLevels() {
        StringBuilder parentPathBuidler = new StringBuilder().append(ngffDir);
        if(!ngffDir.endsWith("/")) {
            parentPathBuidler.append("/");
        }
        parentPathBuidler.append(filesetId).append(".tiledb").append("/")
            .append(pixels.getImage().getSeries()).append("/");
        try {
            return TiledbUtils.getResolutionLevelCountS3(parentPathBuidler.toString(), accessKey,
                    secretKey, awsRegion, s3EndpointOverride);
        } catch (TileDBError e) {
            log.error("Error getting s3 TileDB resolution level count for image " + pixels.getImage().getId(), e);
            return -1;
        }
    }

    @Override
    public PixelData getTile(Integer z, Integer c, Integer t, Integer x, Integer y, Integer w, Integer h)
            throws IOException {
        log.info("getTile S3");
        StringBuffer domStrBuf = new StringBuffer();
        domStrBuf.append("[")
            .append(t).append(",")
            .append(c).append(",")
            .append(z).append(",")
            .append(y).append(":").append(y + h).append(",")
            .append(x).append(":").append(x + w).append("]");
        try {
            return TiledbUtils.getImagePixelDataS3(ngffDir, pixels.getImage().getFileset().getId(),
                    pixels.getImage().getSeries(), resolutionLevel,
                    domStrBuf.toString(), Integer.max(w, h),
                    accessKey, secretKey, awsRegion, s3EndpointOverride);
        } catch (TileDBError e) {
            log.error("Error getting image bytes from S3", e);
            return null;
        }
    }

    private int getDimSize(String dimName) {
        StringBuilder s3PathBuilder = new StringBuilder().append(ngffDir);
        if (!ngffDir.endsWith("/")) {
            s3PathBuilder.append("/");
        }
        s3PathBuilder.append(pixels.getImage().getFileset().getId()).append(".tiledb/")
            .append(pixels.getImage().getSeries()).append("/")
            .append(resolutionLevel);
        try (Config config = new Config()) {
            TiledbUtils.setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, s3PathBuilder.toString(), QueryType.TILEDB_READ)){
                Domain dom = array.getSchema().getDomain();
                return (int) ((long) dom.getDimension(dimName).getDomain().getSecond()
                        - (long) dom.getDimension(dimName).getDomain().getFirst() + 1);
            }
        } catch (TileDBError e) {
            log.error("TileDBError in TiledbS3PixelBuffer", e);
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getResolutionLevel() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setResolutionLevel(int resolutionLevel) {
        // TODO Auto-generated method stub

    }

    @Override
    public Dimension getTileSize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<List<Integer>> getResolutionDescriptions() {
        int resLevels = getResolutionLevels();
        List<List<Integer>> resolutionDescriptions = new ArrayList<List<Integer>>();
        int originalResolution = resolutionLevel;
        for(int i = 0; i < resLevels; i++) {
            this.resolutionLevel = i;
            List<Integer> description = new ArrayList<Integer>();
            description.add(getSizeX());
            description.add(getSizeY());
            resolutionDescriptions.add(description);
        }
        setResolutionLevel(originalResolution);
        return resolutionDescriptions;
    }

}
