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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.imageio.ImageIO;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import ome.util.PixelData;
import ome.xml.model.primitives.Color;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.model.MaskI;
import omero.model.Shape;
import omero.sys.ParametersI;

public class ShapeMaskRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    private static final String CAN_READ_EVENT =
            "omero.can_read";

    private static final String GET_OBJECT_EVENT =
            "omero.get_object";


    /** Shape mask context */
    private final ShapeMaskCtx shapeMaskCtx;

    /** Vertx reference */
    private final Vertx vertx;

    /**
     * Default constructor.
     * @param shapeMaskCtx {@link ShapeMaskCtx} object
     */
    public ShapeMaskRequestHandler(ShapeMaskCtx shapeMaskCtx,
            Vertx vertx) {
        log.info("Setting up handler");
        this.shapeMaskCtx = shapeMaskCtx;
        this.vertx = vertx;
    }

    public CompletableFuture<byte[]> renderShapeMask(String sessionKey) {
        CompletableFuture<byte[]> promise = new CompletableFuture<byte[]>();
        getMask(sessionKey, shapeMaskCtx.shapeId)
        .thenAccept(mask -> {
            try {
                if (mask != null) {
                    promise.complete(renderShapeMask(mask));
                }
            } catch (Exception e) {
                log.error("Exception while retrieving shape mask", e);
                promise.completeExceptionally(e);
            }
        });
        return promise;
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
     * @see {@link #NotByteAligned(Color, byte[], int, int)}
     */
    protected byte[] renderShapeMask(
            Color fillColor, byte[] bytes, int width, int height)
                    throws IOException {
        StopWatch t0 = new Slf4JStopWatch("renderShapeMask");
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
            t0.stop();
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

    public CompletableFuture<Boolean> canRead(String sessionKey) {
        CompletableFuture<Boolean> promise = new CompletableFuture<Boolean>();
        String type = Shape.class.getName();
        long id = shapeMaskCtx.shapeId;
        log.debug("Type: {} Id: {}", type, id);

        final Map<String, Object> data = new HashMap<String, Object>();
        data.put("sessionKey", sessionKey);
        data.put("type", type);
        data.put("id", id);
        vertx.eventBus().<Boolean>send(
                CAN_READ_EVENT,
                Json.encode(data), result -> {
            String s = "";
            if (result.failed()) {
                promise.completeExceptionally(result.cause());
                return;
            }
            promise.complete(result.result().body());
        });
        return promise;
    }

    protected CompletableFuture<MaskI> getMask(String sessionKey, Long shapeId) {
        CompletableFuture<MaskI> promise = new CompletableFuture<MaskI>();
        String type = Shape.class.getName();
        //Get the mask
        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("type", type);
        data.put("id", shapeId);
        StopWatch t0 = new Slf4JStopWatch("getMask");
        vertx.eventBus().<byte[]>send(GET_OBJECT_EVENT, data, result -> {
            try {
                if (result.failed()) {
                    promise.completeExceptionally(result.cause());
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                MaskI mask = (MaskI) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);
            } finally {
                t0.stop();
            }
        });
        return promise;
    }
}
