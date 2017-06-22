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

import static omero.rtypes.unwrap;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;

import omero.ServerError;
import omero.api.RenderingEnginePrx;
import omero.model.RenderingModel;
import omero.model.IObject;
import omero.model.Image;
import omero.romio.PlaneDef;
import omero.romio.RegionDef;
import omero.sys.ParametersI;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    public enum RenderType {JPEG, PNG};

    /** Image Region Context */
    private final ImageRegionCtx imageRegionCtx;

    /** Image Region Context */
    private final RenderType renderType;

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx, RenderType renderType) {
        log.info("Setting up handler");
        this.imageRegionCtx = imageRegionCtx;
        this.renderType = renderType;
    }

    /**
     * Render Image region event handler. Responds with a
     * <code>image/jpeg</code> body on success based on the
     * <code>imageId</code>, <code>z</code> and <code>t</code> encoded in the
     * URL or HTTP 404 if the {@link Image}
     * does not exist or the user does not have permissions to access it.
     * @param event Current routing context.
     */
    public byte[] renderImageRegion(omero.client client) {
        StopWatch t0 = new Slf4JStopWatch("renderImageRegion");
        try {
            Image image = getImage(client, imageRegionCtx.imageId);
            if (image != null) {
                return getRegion(client, image);
            } else {
                log.debug("Cannot find Image:{}", imageRegionCtx.imageId);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving thumbnail", e);
        } finally {
            t0.stop();
        }
        return null;
    }

    /**
     * Retrieves a single {@link Image} from the server.
     * @param client OMERO client to use for querying.
     * @param imageId {@link Image} identifier to query for.
     * @return Loaded {@link Image} and primary {@link Pixels} or
     * <code>null</code> if the image does not exist.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    private Image getImage(omero.client client, Long imageId)
            throws ServerError {
        return (Image) getImages(client, Arrays.asList(imageId))
                .stream()
                .findFirst()
                .orElse(null);
    }

    /**
     * Retrieves a single {@link Image} from the server.
     * @param client OMERO client to use for querying.
     * @param imageIds {@link Image} identifiers to query for.
     * @return List of loaded {@link Image} and primary {@link Pixels}.
     * @throws ServerError If there was any sort of error retrieving the images.
     */
    private List<IObject> getImages(omero.client client, List<Long> imageIds)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addIds(imageIds);
        StopWatch t0 = new Slf4JStopWatch("getImages");
        try {
            return client.getSession().getQueryService().findAllByQuery(
                "SELECT i FROM Image as i " +
                "JOIN FETCH i.pixels as p WHERE i.id IN (:ids)",
                params, ctx
            );
        } finally {
            t0.stop();
        }
    }

    /**
     * Retrieves a single JPEG region from the server.
     * @param client OMERO client to use for image region retrieval.
     * @param image {@link Image} instance to retrieve image region for.
     * @return JPEG image region as a byte array.
     * @throws IOException
     * @throws Exception
     */
    private byte[] getRegion(omero.client client, Image image)
            throws IllegalArgumentException, ServerError, IOException {
        log.debug("Getting image region");
        Integer sizeC = (Integer) unwrap(image.getPrimaryPixels().getSizeC());
        Long pixelsId = (Long) unwrap(image.getPrimaryPixels().getId());
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group",
                String.valueOf(unwrap(image.getDetails().getGroup().getId())));
        RenderingEnginePrx renderingEngine =
                client.getSession().createRenderingEngine();
        try {
            StopWatch t0 = new Slf4JStopWatch("RenderingEngine.lookupPixels");
            try {
                renderingEngine.lookupPixels(pixelsId, ctx);
                if (!(renderingEngine.lookupRenderingDef(pixelsId, ctx))) {
                    renderingEngine.resetDefaultSettings(true, ctx);
                    renderingEngine.lookupRenderingDef(pixelsId, ctx);
                }
            } finally {
                t0.stop();
            }
            t0 = new Slf4JStopWatch("RenderingEngine.load");
            try {
                renderingEngine.load(ctx);
            } finally {
                t0.stop();
            }
            PlaneDef pDef = new PlaneDef();
            pDef.z = imageRegionCtx.z;
            pDef.t = imageRegionCtx.t;
            pDef.region = getRegionDef(renderingEngine);
            setRenderingModel(renderingEngine);
            setActiveChannels(renderingEngine, sizeC, ctx);
            setResolutionLevel(renderingEngine);
            setCompressionLevel(renderingEngine);
            t0 = new Slf4JStopWatch("RenderingEngine.renderCompressed");
            try {
                switch (renderType) {
                    case JPEG:
                        return renderingEngine.renderCompressed(pDef);
                    case PNG:
                        int[] buff = renderingEngine.renderAsPackedInt(pDef);
                        BufferedImage img = new BufferedImage(
                                pDef.region.width, pDef.region.height,
                                BufferedImage.TYPE_INT_RGB);
                        img.setRGB(0, 0, pDef.region.width, pDef.region.height,
                                   buff, 0, pDef.region.width);
                        ByteArrayOutputStream baos =
                                new ByteArrayOutputStream();
                        ImageIO.write(img, "png", baos);
                        return baos.toByteArray();
                    default:
                        return null;
                }
            } finally {
                t0.stop();
            }
        } finally {
            renderingEngine.close();
        }
    }

    /**
     * Sets compression level on the <code>renderingEngine</code>
     * @param renderingEngine loaded instance of {@link RenderingEnginePrx}
     * @throws ServerError
     */
    private void setCompressionLevel(RenderingEnginePrx renderingEngine)
            throws ServerError {
        log.debug("Setting compression level: {}",
                  imageRegionCtx.compressionQuality);
        StopWatch t0 =
                new Slf4JStopWatch("RenderingEngine.setCompressionLevel");
        try {
            if (imageRegionCtx.compressionQuality != null) {
                renderingEngine.setCompressionLevel(
                        imageRegionCtx.compressionQuality);
            }
        } finally {
            t0.stop();
        }
    }

    /**
     * Returns RegionDef to read based on tile / region provided in
     * ImageRegionCtx.
     * @param renderingEngine loaded instance of {@link RenderingEnginePrx}
     * @return RegionDef {@link RegionDef} describing image region to read
     * @throws IllegalArgumentException
     * @throws ServerError
     */
    private RegionDef getRegionDef(RenderingEnginePrx renderingEngine)
            throws IllegalArgumentException, ServerError {
        log.debug("Setting region to read");
        RegionDef regionDef = new RegionDef();
        if (imageRegionCtx.tile != null) {
            StopWatch t0 = new Slf4JStopWatch("RenderingEngine.getTileSize");
            int[] tileSize;
            try {
                tileSize = renderingEngine.getTileSize();
            } finally {
                t0.stop();
            }
            regionDef.width = tileSize[0];
            regionDef.height = tileSize[1];
            regionDef.x = imageRegionCtx.tile.getX() * regionDef.width;
            regionDef.y = imageRegionCtx.tile.getY() * regionDef.height;
        } else if (imageRegionCtx.region != null) {
            regionDef.x = imageRegionCtx.region.getX();
            regionDef.y = imageRegionCtx.region.getY();
            regionDef.width = imageRegionCtx.region.getWidth();
            regionDef.height = imageRegionCtx.region.getHeight();
        } else {
            String v = "Tile or region argument required.";
            log.error(v);
            throw new IllegalArgumentException(v);
        }
        return regionDef;
    }

    /**
     * Sets the pyramid resolution level on the <code>renderingEngine</code>
     * @param renderingEngine loaded instance of {@link RenderingEnginePrx}
     * @throws ServerError
     */
    private void setResolutionLevel(RenderingEnginePrx renderingEngine)
            throws ServerError {
        log.debug("Setting resolution level: {}", imageRegionCtx.resolution);
        if (imageRegionCtx.resolution == null) {
            return;
        }
        StopWatch t0 =
                new Slf4JStopWatch("RenderingEngine.getResolutionLevels");
        Integer numberOfLevels;
        try {
            numberOfLevels = renderingEngine.getResolutionLevels();
        } finally {
            t0.stop();
        }
        Integer level = numberOfLevels - imageRegionCtx.resolution - 1;
        log.debug("Setting resolution level to: {}", level);
        t0 = new Slf4JStopWatch("RenderingEngine.setResolutionLevel");
        try {
            renderingEngine.setResolutionLevel(level);
        } finally {
            t0.stop();
        }
    }

    /**
     * Sets the rendering model on <code>renderingEngine</code>
     * @param renderingEngine loaded instance of {@link RenderingEnginePrx}
     * @throws ServerError
     */
    private void setRenderingModel(RenderingEnginePrx renderingEngine)
            throws ServerError {
        log.debug("Setting rendering model: {}", imageRegionCtx.m);
        StopWatch t0 = new Slf4JStopWatch("RenderingEngine.getAvailableModels");
        List<RenderingModel> renderingModels;
        try {
            renderingModels = renderingEngine.getAvailableModels()
                    .stream()
                    .map(x -> (RenderingModel) x)
                    .collect(Collectors.toList());
        } finally {
            t0.stop();
        }
        for (RenderingModel renderingModel : renderingModels) {
            if (imageRegionCtx.m.equals(unwrap(renderingModel.getValue()))) {
                t0 = new Slf4JStopWatch("RenderingEngine.setModel");
                try {
                    renderingEngine.setModel(renderingModel);
                } finally {
                    t0.stop();
                }
                break;
            }
        }
    }

    /**
     * Sets the rendering settings on the <code>redneringEngine</code> for
     * all the channels (<code>sizeC</code>).
     * @param renderingEngine loaded instance of {@link RenderingEnginePrx}
     * @param sizeC number of channels
     * @param ctx OMERO context (group)
     * @throws ServerError
     */
    private void setActiveChannels(
            RenderingEnginePrx renderingEngine, int sizeC,
            Map<String, String> ctx)
                    throws ServerError {
        log.debug("Setting active channels");
        int idx = 0; // index of windows/colors args
        for (int c = 0; c < sizeC; c++) {
            StopWatch t0 = new Slf4JStopWatch("RenderingEngine.setActive");
            try {
                renderingEngine.setActive(
                        c, imageRegionCtx.channels.contains(c + 1), ctx);
            } finally {
                t0.stop();
            }
            if (!imageRegionCtx.channels.contains(c + 1)) {
                if (imageRegionCtx.channels.contains(-1 * (c + 1))) {
                    idx += 1;
                }
                continue;
            }
            if (imageRegionCtx.windows != null) {
                float min = (float) imageRegionCtx.windows.get(idx)[0];
                float max = (float) imageRegionCtx.windows.get(idx)[1];
                log.debug("Channel: {}, [{}, {}]", c, min, max);
                t0 = new Slf4JStopWatch("RenderingEngine.setChannelWindow");
                try {
                    renderingEngine.setChannelWindow(c, min, max, ctx);
                } finally {
                    t0.stop();
                }
            }
            if (imageRegionCtx.colors != null) {
                int[] rgba = splitHTMLColor(imageRegionCtx.colors.get(idx));
                if (rgba != null) {
                    t0 = new Slf4JStopWatch("RenderingEngine.setRGBA");
                    try {
                        renderingEngine.setRGBA(
                                c, rgba[0], rgba[1], rgba[2], rgba[3], ctx);
                    } finally {
                        t0.stop();
                    }
                }
            }
            idx += 1;
        }
    }

    /**
     *  Splits an hex stream of characters into an array of bytes
     *  in format (R,G,B,A).
     *  - abc      -> (0xAA, 0xBB, 0xCC, 0xFF)
     *  - abcd     -> (0xAA, 0xBB, 0xCC, 0xDD)
     *  - abbccd   -> (0xAB, 0xBC, 0xCD, 0xFF)
     *  - abbccdde -> (0xAB, 0xBC, 0xCD, 0xDE)
     *  @param color: Characters to split.
     *  @return rgba - list of Ints
     */
    private int[] splitHTMLColor(String color) {
        List<Integer> level1 = Arrays.asList(3, 4);
        int[] out = new int[4];
        try {
            if (level1.contains(color.length())) {
                String c = color;
                color = "";
                for (char ch : c.toCharArray()) {
                    color += ch + ch;
                }
            }
            if (color.length() == 6) {
                color += "FF";
            }
            if (color.length() == 8) {
                out[0] = Integer.parseInt(color.substring(0, 2), 16);
                out[1] = Integer.parseInt(color.substring(2, 4), 16);
                out[2] = Integer.parseInt(color.substring(4, 6), 16);
                out[3] = Integer.parseInt(color.substring(6, 8), 16);
                return out;
            }
        } catch (Exception e) {
            log.error("Error while parsing color: {}", color, e);
        }
        return null;
    }
}
