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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.imageio.ImageIO;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import ome.util.PixelData;
import ome.xml.model.primitives.Color;
import ome.model.roi.Mask;

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
    public ShapeMaskRequestHandler(ShapeMaskCtx shapeMaskCtx, Vertx vertx) {
        this.shapeMaskCtx = shapeMaskCtx;
        this.vertx = vertx;
    }

    public CompletableFuture<byte[]> renderShapeMask() {
        CompletableFuture<byte[]> promise = new CompletableFuture<byte[]>();
        getMask().thenAccept(mask -> {
            try {
                if (mask != null) {
                    promise.complete(renderShapeMask(mask));
                }
            } catch (Exception e) {
                log.error("Exception while rendering shape mask", e);
                promise.completeExceptionally(e);
            }
        });
        return promise;
    }

    /**
     * Render shape mask.
     * @param mask mask to render
     * @return <code>image/png</code> encoded mask
     * @throws IOException
     */
    protected byte[] renderShapeMask(Mask mask) throws IOException {
        Color fillColor = Optional.ofNullable(mask.getFillColor())
            .map(x -> new Color(x))
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
        int width = (int) mask.getWidth().intValue();
        int height = (int) mask.getHeight().intValue();
        return renderShapeMask(fillColor, bytes, width, height);
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

    public CompletableFuture<Boolean> canRead() {
        CompletableFuture<Boolean> promise = new CompletableFuture<Boolean>();
        String type = "Mask";
        long id = shapeMaskCtx.shapeId;
        log.info("canRead Type: {} Id: {}", type, id);

        final JsonObject data = new JsonObject();
        data.put("sessionKey", shapeMaskCtx.omeroSessionKey);
        data.put("type", type);
        data.put("id", id);
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("can_read");
        vertx.eventBus().<Boolean>request(CAN_READ_EVENT, data, result -> {
            if (result.failed()) {
                span.finish();
                promise.completeExceptionally(result.cause());
                return;
            }
            span.finish();
            promise.complete(result.result().body());
        });
        return promise;
    }

    protected CompletableFuture<Mask> getMask() {
        CompletableFuture<Mask> promise = new CompletableFuture<Mask>();
        String type = "Mask";
        long id = shapeMaskCtx.shapeId;
        //Get the mask
        final JsonObject data = new JsonObject();
        data.put("sessionKey", shapeMaskCtx.omeroSessionKey);
        data.put("type", type);
        data.put("id", id);
        log.info("getMask Type: {} Id: {}", type, id);
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_mask");
        vertx.eventBus().<byte[]>request(GET_OBJECT_EVENT, data, result -> {
            try {
                if (result.failed()) {
                    span.finish();
                    promise.completeExceptionally(result.cause());
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                Mask mask = (Mask) ois.readObject();
                span.finish();
                promise.complete(mask);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                span.finish();
                promise.completeExceptionally(e);
            }
        });
        return promise;
    }
}
