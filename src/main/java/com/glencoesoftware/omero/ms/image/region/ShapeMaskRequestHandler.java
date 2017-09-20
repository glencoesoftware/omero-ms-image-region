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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.imageio.ImageIO;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;

import ome.xml.model.primitives.Color;
import omero.ServerError;
import omero.model.MaskI;
import omero.sys.ParametersI;

public class ShapeMaskRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    /** Shape mask context */
    private final ShapeMaskCtx shapeMaskCtx;

    /**
     * Default constructor.
     * @param shapeMaskCtx {@link ShapeMaskCtx} object
     */
    public ShapeMaskRequestHandler(ShapeMaskCtx shapeMaskCtx) {
        log.info("Setting up handler");
        this.shapeMaskCtx = shapeMaskCtx;
    }

    /**
     * Render shape mask event handler. Responds with a
     * <code>image/png</code> body on success based on the
     * <code>shapeId</code> encoded in the URL or HTTP 404 if the {@link Shape}
     * does not exist or the user does not have permissions to access it.
     * @param event Current routing context.
     */
    public byte[] renderShapeMask(omero.client client) {
        StopWatch t0 = new Slf4JStopWatch("renderShapeMask");
        try {
            MaskI mask = getMask(client, shapeMaskCtx.shapeId);
            if (mask != null) {
                Color fillColor = Optional.ofNullable(mask.getFillColor())
                    .map(x -> new Color(x.getValue()))
                    .orElse(new Color(255, 255, 0, 255));
                if (shapeMaskCtx.color != null) {
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

                // Create buffered image
                DataBuffer dataBuffer = new DataBufferByte(bytes, bytes.length);
                WritableRaster raster = Raster.createPackedRaster(
                        dataBuffer,
                        (int) mask.getWidth().getValue(),
                        (int) mask.getHeight().getValue(),
                        1, new Point(0, 0));
                byte[] colorMap = new byte[] {
                    // First index (0); 100% transparent
                    0, 0, 0, 0, 
                    // Second index (1); from shape 
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
            }
            log.debug("Cannot find Shape:{}", shapeMaskCtx.shapeId);
        } catch (Exception e) {
            log.error("Exception while retrieving shape mask", e);
        } finally {
            t0.stop();
        }
        return null;
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
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(shapeId);
        StopWatch t0 = new Slf4JStopWatch("getMask");
        try {
            return (MaskI) client.getSession().getQueryService().findByQuery(
                "SELECT s FROM Shape as s " +
                "WHERE s.id = :id", params, ctx
            );
        } finally {
            t0.stop();
        }
    }

}
