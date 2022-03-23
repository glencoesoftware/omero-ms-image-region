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
import java.awt.image.IndexColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.imageio.ImageIO;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import loci.formats.FormatTools;
import ome.io.nio.PixelBuffer;
import ome.util.PixelData;
import ome.xml.model.primitives.Color;
import omero.ApiUsageException;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.model.ExternalInfo;
import omero.model.Mask;
import omero.model.MaskI;
import omero.model.Pixels;
import omero.sys.ParametersI;
import omero.util.IceMapper;

import static omero.rtypes.unwrap;

public class ShapeMaskRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    /** Shape mask context */
    private final ShapeMaskCtx shapeMaskCtx;

    /** Configured Pixels service */
    private final PixelsService pixelsService;

    /**
     * Default constructor.
     * @param shapeMaskCtx {@link ShapeMaskCtx} object
     * @param pixelsService configured pixels service
     */
    public ShapeMaskRequestHandler(
            ShapeMaskCtx shapeMaskCtx, PixelsService pixelsService) {
        log.info("Setting up handler");
        this.shapeMaskCtx = shapeMaskCtx;
        this.pixelsService = pixelsService;
    }

    /**
     * Render shape mask request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>shapeMaskCtx</code>.
     */
    public byte[] renderShapeMask(omero.client client) {
        try {
            Mask mask = getMask(client, shapeMaskCtx.shapeId);
            if (mask != null) {
                return renderShapeMask(mask);
            }
            log.debug("Cannot find Shape:{}", shapeMaskCtx.shapeId);
        } catch (Exception e) {
            log.error("Exception while retrieving shape mask", e);
        }
        return null;
    }

    /**
     * Render shape mask.
     * @param mask mask to render
     * @return <code>image/png</code> encoded mask
     */
    protected byte[] renderShapeMask(Mask mask) {
        try {
            Color fillColor = Optional.ofNullable(mask.getFillColor())
                .map(x -> new Color(x.getValue()))
                .orElse(new Color(255, 255, 0, 255));
            if (shapeMaskCtx.color != null) {
                // Color came from the request so we override the default
                // color the mask was assigned.
                fillColor = ImageRegionCtx.splitHTMLColor(shapeMaskCtx.color);
            }
            log.debug(
                "Fill color Red:{} Green:{} Blue:{} Alpha:{}",
                fillColor.getRed(), fillColor.getGreen(),
                fillColor.getBlue(), fillColor.getAlpha()
            );
            byte[] bytes = getShapeMaskBytes(mask);
            return renderShapeMask(mask, fillColor, bytes);
        } catch (Exception e) {
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


    /**
     * Render shape mask.
     * @param mask mask to render
     * @param fillColor fill color to use for the mask
     * @param bytes mask bytes to render
     * @return <code>image/png</code> encoded mask
     * @see {@link #renderShapeMaskNotByteAligned(Color, byte[], int, int)}
     */
    protected byte[] renderShapeMask(Mask mask, Color fillColor, byte[] bytes)
            throws IOException {
        ScopedSpan span = null;
        if (Tracing.currentTracer() != null) {
            span =
                Tracing.currentTracer().startScopedSpan("render_shape_mask");
        }
        try {
            // The underlying raster will used a MultiPixelPackedSampleModel
            // which expects the row stride to be evenly divisible by the byte
            // width of the data type.  If it is not so aligned or is coming
            // from an NGFF source and will not be packed bits we will need
            // to convert it to a byte mask for rendering.
            String uuid = getUuid(mask);
            int bitsPerPixel = 1;
            int width = (int) mask.getWidth().getValue();
            int height = (int) mask.getHeight().getValue();
            if (width % 8 != 0 || uuid != null) {
                bytes = convertToBytes(bytes, width * height);
                bitsPerPixel = 8;
            }
            bytes = flip(bytes, width, height,
                    shapeMaskCtx.flipHorizontal,
                    shapeMaskCtx.flipVertical);
            log.debug("Rendering Mask Width:{} Height:{} bitsPerPixel:{} " +
                    "Size:{}", width, height, bitsPerPixel, bytes.length);
            // Create buffered image
            DataBuffer dataBuffer = new DataBufferByte(bytes, bytes.length);
            WritableRaster raster = Raster.createPackedRaster(
                    dataBuffer, width, height, bitsPerPixel, new Point(0, 0));
            byte[] colorMap = new byte[] {
                // First index (0); 100% transparent
                0, 0, 0, 0,
                // Second index (1); our color of choice
                (byte) fillColor.getRed(), (byte) fillColor.getGreen(),
                (byte) fillColor.getBlue(), (byte) fillColor.getAlpha()
            };
            ColorModel colorModel = new IndexColorModel(
                    1, 2, colorMap, 0, true);
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

    /**
     * Wrap bytes as pixel data for generic retrieval by byte width.
     * @param source source bytes to wrap
     * @param size number of pixels <code>source</code> corresponds to
     * @return See above.
     */
    private PixelData asPixelData(byte[] source, int size) {
        int bytesPerPixel = source.length / size;
        ByteBuffer asByteBuffer = ByteBuffer.wrap(source);
        switch (bytesPerPixel) {
            case 1:
                return new PixelData("uint8", asByteBuffer);
            case 2:
                return new PixelData("uint16", asByteBuffer);
            case 4:
                return new PixelData("uint32", asByteBuffer);
            default:
                return new PixelData("bit", asByteBuffer);
        }
    }

    /**
     * Converts data to a <code>[0, 1]</code> byte mask.
     * @param source the data to convert
     * @param size number of elements to convert
     */
    private byte[] convertToBytes(byte[] source, int size) {
        PixelData v = asPixelData(source, size);
        byte[] destination = new byte[size];
        for (int i = 0; i < size; i++) {
            destination[i] = (byte) (v.getPixelValue(i) == 0? 0 : 1);
        }
        return destination;
    }

    /**
     * Whether or not a single {@link MaskI} can be read from the server.
     * @param client OMERO client to use for querying.
     * @return <code>true</code> if the {@link Mask} can be loaded or
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
                            "SELECT s.id FROM Shape AS s " +
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
     * Retrieves a single {@link Mask} from the server.
     * @param client OMERO client to use for querying.
     * @param shapeId {@link Mask} identifier to query for.
     * @return Loaded {@link Mask} or <code>null</code> if the shape does not
     * exist or the user does not have permissions to access it.
     * @throws ServerError If there was any sort of error retrieving the mask.
     */
    protected Mask getMask(omero.client client, Long shapeId)
            throws ServerError {
        return getMask(client.getSession().getQueryService(), shapeId);
    }

    /**
     * Retrieves a single {@link Mask} from the server.
     * @param iQuery OMERO query service to use for metadata access.
     * @param shapeId {@link Mask} identifier to query for.
     * @return Loaded {@link Mask} or <code>null</code> if the shape does not
     * exist or the user does not have permissions to access it.
     * @throws ServerError If there was any sort of error retrieving the mask.
     */
    protected Mask getMask(IQueryPrx iQuery, Long shapeId)
            throws ServerError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_mask");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            ParametersI params = new ParametersI();
            params.addId(shapeId);
            log.info("Getting mask for shape id {}", Long.toString(shapeId));
            return (Mask) iQuery.findByQuery(
                "SELECT s FROM Shape s " +
                "JOIN FETCH s.roi AS roi " +
                "JOIN FETCH roi.image AS image " +
                "LEFT OUTER JOIN FETCH image.wellSamples as ws " +
                "LEFT OUTER JOIN FETCH ws.well as w " +
                "LEFT OUTER JOIN FETCH w.wellSamples " +
                "JOIN FETCH image.pixels AS pixels " +
                "LEFT OUTER JOIN FETCH s.details.externalInfo " +
                "WHERE s.id = :id", params, ctx
            );
        } finally {
            span.finish();
        }
    }

    /**
     * Retrieve {@link Mask} UUID.
     * @param mask loaded {@link Mask} to check for a UUID
     * @return UUID or <code>null</code> if the mask does not contain a UUID
     * in its {@link ExternalInfo}.
     */
    private String getUuid(Mask mask) {
        ExternalInfo externalInfo = mask.getDetails().getExternalInfo();
        if (externalInfo == null) {
            log.debug("Shape:{} missing ExternalInfo", unwrap(mask.getId()));
            return null;
        }
        String uuid = (String) unwrap(externalInfo.getUuid());
        if (uuid == null) {
            log.debug("Shape:{} missing UUID", unwrap(mask.getId()));
            return null;
        }
        return uuid;
    }

    /**
     * Get shape mask bytes request handler.
     * @param mask loaded {@link Mask} to retrieve the mask bytes for.
     * @return Mask bytes either from the database or NGFF.
     * @throws IOException
     * @throws ApiUsageException
     */
    private byte[] getShapeMaskBytes(Mask mask)
            throws ApiUsageException, IOException {
        String uuid = getUuid(mask);
        if (uuid == null) {
            return mask.getBytes();
        }
        PixelBuffer pixelBuffer = pixelsService.getLabelImagePixelBuffer(
                (ome.model.core.Pixels) new IceMapper().reverse(
                        mask.getRoi().getImage().getPrimaryPixels()),
                uuid);
        int resolutionLevel =
                shapeMaskCtx.resolution == null ? 0
                        : shapeMaskCtx.resolution;
        resolutionLevel = Math.abs(
                resolutionLevel - (pixelBuffer.getResolutionLevels() - 1));
        pixelBuffer.setResolutionLevel(resolutionLevel);
        String domain = shapeMaskCtx.subarrayDomainStr;
        if (domain == null) {
            int t = Optional.ofNullable(
                    (Integer) unwrap(mask.getTheT())).orElse(0);
            int c = Optional.ofNullable(
                    (Integer) unwrap(mask.getTheC())).orElse(0);
            int z = Optional.ofNullable(
                    (Integer) unwrap(mask.getTheZ())).orElse(0);
            Integer y0 = ((Double) unwrap(mask.getY())).intValue();
            Integer y1 = ((Double) unwrap(mask.getHeight())).intValue();
            Integer x0 = ((Double) unwrap(mask.getX())).intValue();
            Integer x1 = ((Double) unwrap(mask.getWidth())).intValue();
            domain = String.format(
                    "[%d:%d,%d:%d,%d:%d,%d:%d,%d:%d]",
                    t, t, c, c, z, z, y0, y1, x0, x1);
        }
        int[][] shapesAndOffsets =
                pixelsService.getShapeAndStartFromString(domain);
        clampShapes(shapesAndOffsets, pixelBuffer);
        int sizeT = shapesAndOffsets[0][0];
        int sizeC = shapesAndOffsets[0][1];
        int sizeZ = shapesAndOffsets[0][2];
        int sizeY = shapesAndOffsets[0][3];
        int sizeX = shapesAndOffsets[0][4];
        if (sizeT > 1) {
            throw new IllegalArgumentException(
                    "SizeT " + sizeT + " > 1 shape mask bytes retrieval " +
                    "is not supported");
        }
        if (sizeC > 1) {
            throw new IllegalArgumentException(
                    "SizeC " + sizeC + " > 1 shape mask bytes retrieval " +
                    "is not supported");
        }
        if (sizeZ > 1) {
            throw new IllegalArgumentException(
                    "SizeZ " + sizeZ + " > 1 shape mask bytes retrieval " +
                    "is not supported");
        }
        int t = shapesAndOffsets[1][0];
        int c = shapesAndOffsets[1][1];
        int z = shapesAndOffsets[1][2];
        int y = shapesAndOffsets[1][3];
        int x = shapesAndOffsets[1][4];
        return pixelBuffer
                .getTile(z, c, t, x, y, sizeX, sizeY)
                .getData()
                .array();
    }

    /**
     * If necessary and possible, truncate the shape so that it fits
     * within the bounds of the image at the current resolution level
     * @param shapesAndOffsets
     * @param pixelBuffer
     */
    private void clampShapes(int[][] shapesAndOffsets, PixelBuffer pixelBuffer) {
        if (shapesAndOffsets[1][4] >= pixelBuffer.getSizeX()) {
            throw new IllegalArgumentException("Requested origin outside image bounds in dimension X");
        }
        if (shapesAndOffsets[1][3] >= pixelBuffer.getSizeY()) {
            throw new IllegalArgumentException("Requested origin outside image bounds in dimension Y");
        }
        shapesAndOffsets[0][4] = Math.min(shapesAndOffsets[0][4],
                pixelBuffer.getSizeX() - shapesAndOffsets[1][4]);
        shapesAndOffsets[0][3] = Math.min(shapesAndOffsets[0][3],
                pixelBuffer.getSizeY() - shapesAndOffsets[1][3]);
    }

    /**
     * Get shape mask bytes request handler.
     * @param client OMERO client to use for querying.
     * @return Mask bytes either from the database or NGFF.
     */
    public byte[] getShapeMaskBytes(omero.client client) {
        try {
            Mask mask = getMask(client, shapeMaskCtx.shapeId);
            if (mask != null) {
                return getShapeMaskBytes(mask);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving shape mask bytes", e);
        }
        return null;
    }

    /**
     * Get label image (NGFF extension to the base {@link Mask}) metadata.
     * @param client OMERO client to use for querying.
     * @return Label image metadata JSON encoded described by the current
     * <code>shapeMaskCtx</code>.
     */
    public JsonObject getLabelImageMetadata(omero.client client) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_label_image_metadata_handler");
        try {
            Mask mask = getMask(client, shapeMaskCtx.shapeId);
            String uuid = getUuid(mask);
            if (uuid == null) {
                throw new IllegalArgumentException(
                        "No UUID for shape " + shapeMaskCtx.shapeId);
            }
            Pixels pixels = mask.getRoi().getImage().getPrimaryPixels();
            ZarrPixelBuffer pixelBuffer = pixelsService.getLabelImagePixelBuffer(
                    (ome.model.core.Pixels) new IceMapper().reverse(pixels),
                    uuid);

            JsonObject metadata = new JsonObject();
            JsonObject multiscalesAsJson = new JsonObject();
            JsonArray datasetsAsJson = new JsonArray();
            metadata.put("multiscales", multiscalesAsJson);

            Map<String, Object> rootGroupAttributes =
                    pixelBuffer.getRootGroupAttributes();
            List<Map<String, String>> datasets = pixelBuffer.getDatasets();
            int[][] chunks = pixelBuffer.getChunks();
            int resolutionLevel = 0;
            for (Map<String, String> dataset : datasets) {
                JsonObject datasetAsJson = new JsonObject();
                datasetAsJson.put("path", dataset.get("path"));
                JsonArray chunksize = new JsonArray();
                for (int chunk : chunks[resolutionLevel]) {
                    chunksize.add(chunk);
                }
                datasetAsJson.put("chunksize", chunksize);
                datasetsAsJson.add(datasetAsJson);
                resolutionLevel++;
            }
            multiscalesAsJson.put("datasets", datasetsAsJson);

            if (rootGroupAttributes.containsKey("minmax")) {
                List<Integer> minMax =
                        (List<Integer>) rootGroupAttributes.get("minmax");
                metadata.put("min", minMax.get(0));
                metadata.put("max", minMax.get(1));
            }

            JsonObject size = new JsonObject();
            size.put("t", pixelBuffer.getSizeT());
            size.put("c", pixelBuffer.getSizeC());
            size.put("z", pixelBuffer.getSizeZ());
            size.put("height", pixelBuffer.getSizeY());
            size.put("width", pixelBuffer.getSizeX());
            metadata.put("size", size);

            metadata.put("uuid", uuid);

            metadata.put("type", FormatTools.getPixelTypeString(
                    pixelBuffer.getPixelsType()));

            return metadata;
        } catch (Exception e) {
            log.error("Exception while retrieving label image metadata", e);
        } finally {
            span.finish();
        }
        return null;
    }

}
