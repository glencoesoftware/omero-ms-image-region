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

import java.awt.Point;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.awt.image.IndexColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.imageio.ImageIO;

import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import io.vertx.core.json.JsonObject;
import ome.util.PixelData;
import ome.xml.model.primitives.Color;
import omeis.providers.re.data.RegionDef;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.model.MaskI;
import omero.sys.ParametersI;

import com.google.gson.Gson;

public class ShapeMaskRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    /** Shape mask context */
    private final ShapeMaskCtx shapeMaskCtx;

    /** Location of label image files */
    private final String labelImagePath;

    /** Label Image Suffix */
    private String labelImageSuffix;

    /** GSON */
    Gson gson;

    /**
     * Default constructor.
     * @param shapeMaskCtx {@link ShapeMaskCtx} object
     */
    public ShapeMaskRequestHandler(ShapeMaskCtx shapeMaskCtx, String labelImagePath, String labelImageSuffix) {
        log.info("Setting up handler");
        this.shapeMaskCtx = shapeMaskCtx;
        this.labelImagePath = labelImagePath;
        this.labelImageSuffix = labelImageSuffix;
    }

    /**
     * Render shape mask request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>shapeMaskCtx</code>.
     */
    public byte[] renderShapeMask(omero.client client) {
        try {
            MaskI mask = getMask(client, shapeMaskCtx.shapeId);
            long imageId = getImageId(client.getSession().getQueryService(), shapeMaskCtx.shapeId);
            if (mask != null) {
                return renderShapeMask(mask, imageId);
            }
            log.debug("Cannot find Shape:{}", shapeMaskCtx.shapeId);
        } catch (Exception e) {
            log.error("Exception while retrieving shape mask", e);
        }
        return null;
    }

    /**
     * Get shape mask bytes request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>shapeMaskCtx</code>.
     */
    public byte[] getShapeMaskBytes(omero.client client) {
        try {
            MaskI mask = getMask(client, shapeMaskCtx.shapeId);
            if (mask != null) {
                if (labelImagePath == null || labelImageSuffix == null) {
                    return mask.getBytes();
                }
                long imageId = getImageId(client.getSession().getQueryService(), shapeMaskCtx.shapeId);
                String uuid = mask.getDetails().getExternalInfo().getUuid().getValue();
                Path labelImageBasePath = Paths.get(labelImagePath).resolve(Long.toString(imageId) + ".tiledb");
                Path labelImageLabelsPath = labelImageBasePath.resolve("0/labels");
                Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
                String resolutionLevel = "0";
                if(shapeMaskCtx.resolution != null) {
                    //Append the resolution level
                    resolutionLevel = Integer.toString(shapeMaskCtx.resolution);
                }
                Path fullLabelImagePath = labelImageShapePath.resolve(resolutionLevel);
                log.info(fullLabelImagePath.toString());
                if (Files.exists(fullLabelImagePath)) {
                    log.info("Getting mask from tiledb for shape " + Long.toString(shapeMaskCtx.shapeId));
                    try (Context ctx = new Context();
                            Array array = new Array(ctx, fullLabelImagePath.toString(), QueryType.TILEDB_READ)){
                            if(shapeMaskCtx.subarrayDomainStr == null) {
                                return getData(array, ctx);
                            } else {
                                return getData(array, ctx, shapeMaskCtx.subarrayDomainStr);
                            }
                    }
                } else {
                    return mask.getBytes();
                }
            }
            log.debug("Cannot find Shape:{}", shapeMaskCtx.shapeId);
        } catch (Exception e) {
            log.error("Exception while retrieving shape mask", e);
        }
        return null;
    }


    private String getStringMetadata(Array array, String key) throws TileDBError {
        if(array.hasMetadataKey(key)) {
            NativeArray strNativeArray = array.getMetadata(key, Datatype.TILEDB_CHAR);
            return new String((byte[]) strNativeArray.toJavaArray(), StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    int getResolutionLevelCount(Path labelImageShapePath) {
        File[] directories = new File(labelImageShapePath.toString()).listFiles(File::isDirectory);
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

    /**
     * Get shape mask bytes request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>shapeMaskCtx</code>.
     */
    public JsonObject getLabelImageMetadata(omero.client client) {
        try {
            if (labelImagePath == null || labelImageSuffix == null) {
                throw new IllegalArgumentException("Label image configs not properly set");
            }
            MaskI mask = getMask(client, shapeMaskCtx.shapeId);
            if (mask != null) {
                long imageId = getImageId(client.getSession().getQueryService(), shapeMaskCtx.shapeId);
                String uuid = mask.getDetails().getExternalInfo().getUuid().getValue();
                Path labelImageBasePath = Paths.get(labelImagePath).resolve(Long.toString(imageId) + ".tiledb");
                Path labelImageLabelsPath = labelImageBasePath.resolve("0/labels");
                Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
                String resolutionLevel = "0";
                if(shapeMaskCtx.resolution != null) {
                    //Append the resolution level
                    resolutionLevel = Integer.toString(shapeMaskCtx.resolution);
                }
                Path fullLabelImagePath = labelImageShapePath.resolve(resolutionLevel);
                log.info(fullLabelImagePath.toString());
                JsonObject multiscales = null;
                if (Files.exists(fullLabelImagePath)) {
                    try (Context ctx = new Context();
                        Array array = new Array(ctx, labelImageShapePath.toString(), QueryType.TILEDB_READ)) {
                            if(array.hasMetadataKey("multiscales")) {
                                String multiscalesMetaStr = getStringMetadata(array, "multiscales");
                                multiscales = new JsonObject(multiscalesMetaStr);
                            }
                        }
                    try (Context ctx = new Context();
                        Array array = new Array(ctx, fullLabelImagePath.toString(), QueryType.TILEDB_READ)){
                        ArraySchema schema = array.getSchema();
                        Domain domain = schema.getDomain();
                        Attribute attribute = schema.getAttribute("a1");

                        int bytesPerPixel = TiledbUtils.getBytesPerPixel(attribute.getType());

                        int num_dims = (int) domain.getNDim();
                        int capacity = 1;
                        long[] subarrayDomain = new long[(int) num_dims*2];
                        for(int i = 0; i < num_dims; i++) {
                            if (domain.getDimension(i).getType() != Datatype.TILEDB_INT64) {
                                throw new IllegalArgumentException("Dimension type "
                                    + domain.getDimension(i).getType().toString() + " not supported");
                            }
                            long start = (long) (domain.getDimension(i).getDomain().getFirst());
                            long end = (long) domain.getDimension(i).getDomain().getSecond();
                            subarrayDomain[i*2] = start;
                            subarrayDomain[i*2 + 1] = end;
                            capacity *= (end - start + 1);
                        }
                        capacity *= bytesPerPixel;

                        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
                        buffer.order(ByteOrder.nativeOrder());
                        JsonObject metadata = new JsonObject();
                        //Dimensions in Dense Arrays must be the same type
                        try (Query query = new Query(array, QueryType.TILEDB_READ);
                                NativeArray subArray = new NativeArray(ctx, subarrayDomain, Datatype.TILEDB_INT64)){
                            query.setSubarray(subArray);
                            query.setBuffer("a1", buffer);
                            query.submit();
                            long[] minMax = TiledbUtils.getMinMax(buffer, attribute.getType());
                            metadata.put("min", minMax[0]);
                            metadata.put("max", minMax[1]);
                            JsonObject size = new JsonObject();
                            size.put("t", (long) domain.getDimension("t").getDomain().getSecond() -
                                    (long) domain.getDimension("t").getDomain().getFirst() + 1);
                            size.put("c", (long) domain.getDimension("c").getDomain().getSecond() -
                                    (long) domain.getDimension("c").getDomain().getFirst() + 1);
                            size.put("z", (long) domain.getDimension("z").getDomain().getSecond() -
                                    (long) domain.getDimension("z").getDomain().getFirst() + 1);
                            size.put("width", (long) domain.getDimension("x").getDomain().getSecond() -
                                    (long) domain.getDimension("x").getDomain().getFirst() + 1);
                            size.put("height", (long) domain.getDimension("y").getDomain().getSecond() -
                                    (long) domain.getDimension("y").getDomain().getFirst() + 1);
                            metadata.put("size", size);
                            metadata.put("type", attribute.getType().toString());
                            if(multiscales != null) {
                                metadata.put("multiscales", multiscales);
                            }
                            metadata.put("uuid", uuid);
                            metadata.put("levels", getResolutionLevelCount(labelImageShapePath));
                        }
                        return metadata;
                    }
                } else {
                    return null;
                }
            }
            log.debug("Cannot find Shape:{}", shapeMaskCtx.shapeId);
        } catch (Exception e) {
            log.error("Exception while retrieving label image metadata", e);
        }
        return null;
    }


    private RegionDef getTruncateRegionDef(RegionDef region,
            int xstart, int xend,
            int ystart, int yend) {
        if (region.getX() > xend ||
                region.getY() > yend ||
                region.getX() < xstart ||
                region.getY() < ystart) {
            throw new IllegalArgumentException("Invalid region");
        }
        return new RegionDef(region.getX(),
                region.getY(),
                (int) Math.min(region.getWidth(), xend - region.getX() + 1),
                (int) Math.min(region.getHeight(), yend - region.getY() + 1));
    }

    private RegionDef getSubregion(Domain domain) throws TileDBError {
        long xstart = (long) (domain.getDimension("x").getDomain().getFirst());
        long xend = (long) domain.getDimension("x").getDomain().getSecond();
        long ystart = (long) (domain.getDimension("y").getDomain().getFirst());
        long yend = (long) domain.getDimension("y").getDomain().getSecond();

        RegionDef region = shapeMaskCtx.tile == null ? shapeMaskCtx.region : shapeMaskCtx.region;
        if (shapeMaskCtx.tile != null) {
            RegionDef tile = shapeMaskCtx.tile;
            int tileWidth = (int) (xend - xstart + 1);
            if(tile.getWidth() > 0 && tile.getWidth() < tileWidth) {
                tileWidth = tile.getWidth();
            }
            int tileHeight = (int) (yend - ystart + 1);
            if(tile.getHeight() > 0 && tile.getHeight() < tileHeight) {
                tileHeight = tile.getHeight();
            }
            int tileX = tile.getX() * tileWidth;
            int tileY = tile.getY() * tileHeight;
            return getTruncateRegionDef(new RegionDef(tileX, tileY, tileWidth, tileHeight),
                    (int) xstart, (int) xend, (int) ystart, (int) yend);
        } else if (shapeMaskCtx.region != null) {
            return getTruncateRegionDef(region, (int) xstart, (int) xend, (int) ystart, (int) yend);
        } else {
            return new RegionDef(
                    (int) xstart,
                    (int) ystart,
                    (int) (xend - xstart + 1),
                    (int) (yend - ystart + 1));
        }
    }

    private byte[] renderShapeMaskWithColor(MaskI mask, Color fillColor) throws IOException {
        byte[] bytes = mask.getBytes();
        int width = (int) mask.getWidth().getValue();
        int height = (int) mask.getHeight().getValue();
        // The underlying raster will used a MultiPixelPackedSampleModel
        // which expects the row stride to be evenly divisible by the byte
        // width of the data type.  If it is not so aligned we will need
        // to convert it to a byte mask for rendering.
        int bitsPerPixel = 1;
        if (width % 8 != 0) {
            bytes = convertBitsToBytes(bytes, width * height);
            bitsPerPixel = 8;
        }
        byte[] colorMap = new byte[] {
                // First index (0); 100% transparent
                0, 0, 0, 0,
                // Second index (1); our color of choice
                (byte) fillColor.getRed(), (byte) fillColor.getGreen(),
                (byte) fillColor.getBlue(), (byte) fillColor.getAlpha()
            };
        return renderShapeMask(fillColor, bytes, width, height, bitsPerPixel, colorMap);
    }

    /**
     * Render shape mask.
     * @param mask mask to render
     * @return <code>image/png</code> encoded mask
     */
    protected byte[] renderShapeMask(MaskI mask, long imageId) {
        try {
            Color fillColor = Optional.ofNullable(mask.getFillColor())
                .map(x -> new Color(x.getValue()))
                .orElse(new Color(255, 255, 0, 255));
            if (shapeMaskCtx.color != null) {
                // Color came from the request so we override the default
                // color the mask was assigned.
                int[] rgba = ImageRegionRequestHandler
                        .splitHTMLColor(shapeMaskCtx.color);
                fillColor = new Color(rgba[0], rgba[1], rgba[2], rgba[3]);
            }
            log.debug(
                "Fill color Red:{} Green:{} Blue:{} Alpha:{}",
                fillColor.getRed(), fillColor.getGreen(),
                fillColor.getBlue(), fillColor.getAlpha()
            );
            if (labelImagePath == null && labelImageSuffix == null) {
                renderShapeMaskWithColor(mask, fillColor);
            }
            //If the path to the label image exists, get it
            String uuid = mask.getDetails().getExternalInfo().getUuid().getValue();
            Path labelImageBasePath = Paths.get(labelImagePath).resolve(Long.toString(imageId) + ".tiledb");
            log.info(labelImageBasePath.toString());
            Path labelImageLabelsPath = labelImageBasePath.resolve("0/labels");
            Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
            log.info(labelImageShapePath.toString());
            String resolutionLevel = "0";
            if(shapeMaskCtx.resolution != null) {
                //Append the resolution level
                resolutionLevel = Integer.toString(shapeMaskCtx.resolution);
            }
            Path fullLabelImagePath = labelImageShapePath.resolve(resolutionLevel);
            log.info(fullLabelImagePath.toString());
            if (Files.exists(fullLabelImagePath)) {
                log.info("Getting mask from tiledb for shape " + Long.toString(shapeMaskCtx.shapeId));
                try (Context ctx = new Context();
                        Array array = new Array(ctx, fullLabelImagePath.toString(), QueryType.TILEDB_READ)){
                        byte[] tiledbBytes = null;
                        if(shapeMaskCtx.subarrayDomainStr == null) {
                            tiledbBytes = getData(array, ctx);
                        } else {
                            tiledbBytes = getData(array, ctx, shapeMaskCtx.subarrayDomainStr);
                        }
                        Domain domain = array.getSchema().getDomain();

                        RegionDef subRegion = getSubregion(domain);
                        Datatype type = array.getSchema().getAttribute("a1").getType();
                        int bitsPerPixel = 8 * TiledbUtils.getBytesPerPixel(type);
                        byte[] colorMap = null;
                        if(type == Datatype.TILEDB_UINT8)  {
                            colorMap = getColorMap(TiledbUtils.getMaxUnsignedByte(tiledbBytes));
                        } else if (type == Datatype.TILEDB_UINT16) {
                            colorMap = getColorMap(TiledbUtils.getMaxUnsignedShort(tiledbBytes));
                        } else {
                            throw new IllegalArgumentException("Currently unsupported data type: " + type.toString());
                        }
                        return renderShapeMask(fillColor, tiledbBytes, subRegion.getWidth(), subRegion.getHeight(), bitsPerPixel, colorMap);
                } catch (TileDBError e) {
                    log.error("Caught TileDBError", e);
                    log.error("Getting standard shape mask");
                    return renderShapeMaskWithColor(mask, fillColor);
                }
            } else {
                return renderShapeMaskWithColor(mask, fillColor);
            }
        } catch (IOException e) {
            log.error("Exception while rendering shape mask", e);
        }
        return null;
    }

    /**
     * Flip an image horizontally, vertically, or both.
     * @param src source image buffer
     * @param sizeX size of <code>src</code> in X (number of columns)
     * @param sizeY size of <code>src</code> in Y (number of rows)
     * @param flipHorizontal whether or not to flip the image horizontally
     * @param flipVertical whether or not to flip the image vertically
     * @return Newly allocated buffer with flipping applied or <code>src</code>
     * if no flipping has been requested.
     */
    public static byte[] flip(
            byte[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        if (!flipHorizontal && !flipVertical) {
            return src;
        }

        if (src == null) {
            throw new IllegalArgumentException("Attempted to flip null image");
        } else if (sizeX == 0 || sizeY == 0) {
            throw new IllegalArgumentException("Attempted to flip image with 0 size");
        }

        byte[] dest = new byte[src.length];
        int srcIndex, destIndex;
        int xOffset = flipHorizontal? sizeX : 1;
        int yOffset = flipVertical? sizeY : 1;
        for (int x = 0; x < sizeX; x++) {
            for (int y = 0; y < sizeY; y++) {
                srcIndex = (y * sizeX) + x;
                destIndex = Math.abs(((yOffset - y - 1) * sizeX))
                        + Math.abs((xOffset - x - 1));
                dest[destIndex] = src[srcIndex];
            }
        }
        return dest;
    }

    byte[] getColorMap(int maxIndex) {
        int colorMapSize = (maxIndex + 1) * 4;
        byte [] colorMap = new byte[colorMapSize];
        int[] coprimes = new int[] {111, 51, 29};
        int k = 0;
        for (int i = 1; i < maxIndex + 1; i++) {
            for (int j = 0; j < 4; j++) {
                if (j != 3) {
                    if (j == k) {
                        colorMap[i*4 + j] = (byte) (coprimes[j]*(i/4 + 1) % 256);
                    } else {
                        colorMap[i*4 + j] = colorMap[(i-1)*4 +j];
                    }
                } else {
                    colorMap[i*4 + j] = (byte) 255;
                }
            }
            k = (k+1) % 3;
        }
        return colorMap;
    }

    DataBuffer getDataBuffer(byte[] bytes, int bitsPerPixel) {
        switch(bitsPerPixel) {
            case 1:
            case 8:
                return new DataBufferByte(bytes, bytes.length);
            case 16:
                ShortBuffer shortBuf = ByteBuffer.wrap(bytes)
                    .order(ByteOrder.nativeOrder())
                    .asShortBuffer();
                short[] shorts = new short[shortBuf.remaining()];
                shortBuf.get(shorts);
                return new DataBufferUShort(shorts, shorts.length);
            case 32:
                IntBuffer intBuf = ByteBuffer.wrap(bytes)
                    .order(ByteOrder.nativeOrder())
                    .asIntBuffer();
                int[] ints = new int[intBuf.remaining()];
                intBuf.get(ints);
                return new DataBufferInt(ints, ints.length);
            default:
                throw new IllegalArgumentException("Unsupported number of bits per pixel: " + Integer.toString(bitsPerPixel));
        }
    }

    /**
     * Render shape mask.
     * @param fillColor fill color to use for the mask
     * @param bytes mask bytes to render
     * @param width width of the mask
     * @param height height of the mask
     * @return <code>image/png</code> encoded mask
     * @see {@link #renderShapeMaskNotByteAligned(Color, byte[], int, int)}
     */
    protected byte[] renderShapeMask(
            Color fillColor, byte[] bytes, int width, int height, int bitsPerPixel, byte[] colorMap)
                    throws IOException {
        for(int i = 0; i < bytes.length; i++) {
            if(bytes[i] != 0) {
                log.info("NONZERO BYTE FOUND!");
                break;
            }
        }
        ScopedSpan span = null;
        if(Tracing.currentTracer() != null) {
            span =
                Tracing.currentTracer().startScopedSpan("render_shape_mask");
        }
        try {
            bytes = flip(bytes, width, height,
                    shapeMaskCtx.flipHorizontal,
                    shapeMaskCtx.flipVertical);
            log.debug("Rendering Mask Width:{} Height:{} bitsPerPixel:{} " +
                    "Size:{}", width, height, bitsPerPixel, bytes.length);
            // Create buffered image
            //DataBuffer dataBuffer = new DataBufferByte(bytes, bytes.length);
            DataBuffer dataBuffer = getDataBuffer(bytes, bitsPerPixel);
            WritableRaster raster = Raster.createPackedRaster(
                    dataBuffer, width, height, bitsPerPixel, new Point(0, 0));
            ColorModel colorModel = new IndexColorModel(
                    bitsPerPixel, colorMap.length/4, colorMap, 0, true);
            BufferedImage image = new BufferedImage(
                    colorModel, raster, false, null);

            // Write PNG to memory and return
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ImageIO.write(image, "png", output);
            return output.toByteArray();
        } finally {
            if(span != null)
            span.finish();
        }
    }

    long[] getFullArrayDomain(Domain domain) throws TileDBError {
        int num_dims = (int) domain.getNDim();
        long[] subarrayDomain = new long[(int) num_dims*2];
        for(int i = 0; i < num_dims; i++) {
            if (domain.getDimension(i).getType() != Datatype.TILEDB_INT64) {
                throw new IllegalArgumentException("Dimension type "
                    + domain.getDimension(i).getType().toString() + " not supported");
            }
            long start = (long) (domain.getDimension(i).getDomain().getFirst());
            long end = (long) domain.getDimension(i).getDomain().getSecond();
            subarrayDomain[i*2] = start;
            subarrayDomain[i*2 + 1] = end;
        }
        return subarrayDomain;
    }

    private byte[] getData(Array array, Context ctx, String subarrayString) throws TileDBError {
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();
        Attribute attribute = schema.getAttribute("a1");

        int bytesPerPixel = TiledbUtils.getBytesPerPixel(attribute.getType());

        int num_dims = (int) domain.getNDim();
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }
        long[] subarrayDomain = TiledbUtils.getSubarrayDomainFromString(subarrayString);
        log.info(Arrays.toString(subarrayDomain));

        int capacity = 1;
        for(int i = 0; i < 5; i++) {
            capacity *= ((int) (subarrayDomain[2*i + 1] - subarrayDomain[2*i] + 1));
        }
        capacity *= bytesPerPixel;
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

        RegionDef subRegion = getSubregion(domain);
        subarrayDomain[6] = subRegion.getY();
        subarrayDomain[7] = subRegion.getY() + subRegion.getHeight() - 1; //Last coordinate is inclusive
        subarrayDomain[8] = subRegion.getX();
        subarrayDomain[9] = subRegion.getX() + subRegion.getWidth() - 1; //Last coordinate is inclusive
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

    /**
     * Converts a bit mask to a <code>[0, 1]</code> byte mask.
     * @param bits the bits to convert
     * @param size number of bits to convert
     */
    protected byte[] convertBitsToBytes(byte[] bits, int size) {
        PixelData bitData = new PixelData("bit", ByteBuffer.wrap(bits));
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) bitData.getPixelValue(i);
        }
        return bytes;
    }

    /**
     * Whether or not a single {@link MaskI} can be read from the server.
     * @param client OMERO client to use for querying.
     * @return <code>true</code> if the {@link MaskI} can be loaded or
     * <code>false</code> otherwise.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    public boolean canRead(omero.client client) {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(shapeMaskCtx.shapeId);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("can_read");
        try {
            List<List<RType>> rows = client.getSession()
                    .getQueryService().projection(
                            "SELECT s.id FROM Shape as s " +
                            "WHERE s.id = :id", params, ctx);
            if (rows.size() > 0) {
                return true;
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while checking shape mask readability", e);
        } finally {
            span.finish();
        }
        return false;
    }

    /**
     * Retrieves a single {@link MaskI} from the server.
     * @param client OMERO client to use for querying.
     * @param shapeId {@link MaskI} identifier to query for.
     * @return Loaded {@link MaskI} or <code>null</code> if the shape does not
     * exist or the user does not have permissions to access it.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    protected MaskI getMask(omero.client client, Long shapeId)
            throws ServerError {
        return getMask(client.getSession().getQueryService(), shapeId);
    }

    /**
     * Retrieves a single {@link MaskI} from the server.
     * @param iQuery OMERO query service to use for metadata access.
     * @param shapeId {@link MaskI} identifier to query for.
     * @return Loaded {@link MaskI} or <code>null</code> if the shape does not
     * exist or the user does not have permissions to access it.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    protected MaskI getMask(IQueryPrx iQuery, Long shapeId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(shapeId);
        log.info("Getting mask for shape id " + Long.toString(shapeId));
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_mask");
        try {
            return (MaskI) iQuery.findByQuery(
                "SELECT s from Shape s join fetch s.details.externalInfo " +
                "WHERE s.id = :id", params, ctx
            );
        } finally {
            span.finish();
        }
    }

    /**
     * Retrieves a image id from the server.
     * @param iQuery OMERO query service to use for metadata access.
     * @param shapeId {@link MaskI} identifier to query for.
     * @return Loaded {@link MaskI} or <code>null</code> if the shape does not
     * exist or the user does not have permissions to access it.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    protected long getImageId(IQueryPrx iQuery, Long shapeId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(shapeId);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_mask");
        try {
            MaskI shape = (MaskI) iQuery.findByQuery(
                "SELECT s FROM Shape as s join fetch s.roi " +
                "WHERE s.id = :id", params, ctx
            );
            return shape.getRoi().getImage().getId().getValue();
        } finally {
            span.finish();
        }
    }
}
