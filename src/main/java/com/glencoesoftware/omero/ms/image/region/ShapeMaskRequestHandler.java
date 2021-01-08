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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.imageio.ImageIO;

import org.slf4j.LoggerFactory;

import com.bc.zarr.ArrayParams;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonObject;
import ome.util.PixelData;
import ome.xml.model.primitives.Color;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.model.Image;
import omero.model.MaskI;
import omero.sys.ParametersI;
import ucar.ma2.InvalidRangeException;

public class ShapeMaskRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    /** Shape mask context */
    private final ShapeMaskCtx shapeMaskCtx;

    /** Location of ngff files */
    private final String ngffDir;

    /** NgffUtils */
    private final NgffUtils ngffUtils;

    /**
     * Default constructor.
     * @param shapeMaskCtx {@link ShapeMaskCtx} object
     */
    public ShapeMaskRequestHandler(ShapeMaskCtx shapeMaskCtx, String ngffDir,
            TiledbUtils tiledbUtils, OmeroZarrUtils zarrUtils) {
        log.info("Setting up handler");
        this.shapeMaskCtx = shapeMaskCtx;
        this.ngffDir = ngffDir;
        this.ngffUtils = new NgffUtils(tiledbUtils, zarrUtils);
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
    protected byte[] renderShapeMask(MaskI mask) {
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
            byte[] bytes = mask.getBytes();
            int width = (int) mask.getWidth().getValue();
            int height = (int) mask.getHeight().getValue();
            return renderShapeMask(fillColor, bytes, width, height);
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
            Color fillColor, byte[] bytes, int width, int height)
                    throws IOException {
        ScopedSpan span = null;
        if(Tracing.currentTracer() != null) {
            span =
                Tracing.currentTracer().startScopedSpan("render_shape_mask");
        }
        try {
            // The underlying raster will used a MultiPixelPackedSampleModel
            // which expects the row stride to be evenly divisible by the byte
            // width of the data type.  If it is not so aligned we will need
            // to convert it to a byte mask for rendering.
            int bitsPerPixel = 1;
            if (width % 8 != 0) {
                bytes = convertBitsToBytes(bytes, width * height);
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
     * Converts a bit mask to a <code>[0, 1]</code> byte mask.
     * @param bits the bits to convert
     * @param size number of bits to convert
     */
    private byte[] convertBitsToBytes(byte[] bits, int size) {
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
                "SELECT s from Shape s left outer join fetch s.details.externalInfo " +
                "WHERE s.id = :id", params, ctx
            );
        } finally {
            span.finish();
        }
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
                if (ngffDir == null) {
                    return mask.getBytes();
                }
                Image image = getImageFromShapeId(client.getSession().getQueryService(), shapeMaskCtx.shapeId);
                if (mask.getDetails().getExternalInfo() == null) {
                    log.error("No UUID associated with shape " + shapeMaskCtx.shapeId);
                    return mask.getBytes();
                }
                String uuid = mask.getDetails().getExternalInfo().getUuid().getValue();
                long filesetId = image.getFileset().getId().getValue();
                int series = image.getSeries().getValue();
                Integer resolution = shapeMaskCtx.resolution == null ? 0 : shapeMaskCtx.resolution;
                if (shapeMaskCtx.subarrayDomainStr == null) {
                    throw new IllegalArgumentException("Failed to supply domain parameter to getShapeMaskBytes");
                }
                try {
                    return ngffUtils.getLabelImageBytes(ngffDir, filesetId, series, uuid, resolution, shapeMaskCtx.subarrayDomainStr);
                } catch (Exception e) {
                    log.error("Error getting label image bytes from TileDB", e);
                    return mask.getBytes();
                }
            }
            log.debug("Cannot find Shape:{}", shapeMaskCtx.shapeId);
        } catch (IllegalArgumentException e) {
            log.error("Missing domain parameter - rethrowing");
            throw e;
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
    public JsonObject getLabelImageMetadata(omero.client client) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_label_image_metadata_handler");
        try {
            if (ngffDir == null) {
                throw new IllegalArgumentException("Label image configs not properly set");
            }
            MaskI mask = getMask(client, shapeMaskCtx.shapeId);
            if (mask != null) {
                Image image = getImageFromShapeId(client.getSession().getQueryService(), shapeMaskCtx.shapeId);
                if (mask.getDetails().getExternalInfo() == null) {
                    throw new IllegalArgumentException("No UUID for shape " + shapeMaskCtx.shapeId);
                }
                String uuid = mask.getDetails().getExternalInfo().getUuid().getValue();
                long filesetId = image.getFileset().getId().getValue();
                int series = image.getSeries().getValue();
                int resolution = 0;
                if(shapeMaskCtx.resolution != null) {
                    resolution = shapeMaskCtx.resolution;
                }
                return ngffUtils.getLabelImageMetadata(ngffDir, filesetId, series, uuid, resolution);
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error("Exception while retrieving label image metadata", e);
        } finally {
                span.finish();
        }
        return null;
    }

    /**
     * Retrieves a image id from the server.
     * @param iQuery OMERO query service to use for metadata access.
     * @param shapeId {@link MaskI} identifier to query for.
     * @return Loaded {@link MaskI} or <code>null</code> if the shape does not
     * exist or the user does not have permissions to access it.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    protected Image getImageFromShapeId(IQueryPrx iQuery, Long shapeId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(shapeId);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_mask");
        try {
            MaskI shape = (MaskI) iQuery.findByQuery(
                "SELECT s from Shape s join fetch s.roi roi " +
                "join fetch roi.image " +
                "WHERE s.id = :id", params, ctx
            );
            return shape.getRoi().getImage();
        } finally {
            span.finish();
        }
    }
}
