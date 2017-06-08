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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /** z - index. */
    private final int z;

    /** t -index. */
    private final int t;

    /** Image identifier to request a thumbnail for. */
    private final long imageId;

    /** Resolution level to read */
    private final Integer resolution;

    /** Compression Quality */
    private final Float compressionQuality;

    /** Tile to read */
    private final ArrayList<Integer> tile;

    /** Region to read */
    private final ArrayList<Integer> region;

    /** Inverted axis */
    private final Boolean invertedAxis;

    /** Rendering model */
    private final String model;

    /** Channel settings [-1, 2] **/
    ArrayList<Integer> channels;

    /** Min-max settings for channels **/
    ArrayList<Integer[] > windows;

    /** Channel colors */
    ArrayList<String> colors;

    /**
     * Default constructor.
     * @param z Index of the z section to render the region for.
     * @param t Index of the time point to render the region for.
     */
    public ImageRegionRequestHandler(
            Long imageId, int z, int t,
            ArrayList<Integer> region, ArrayList<Integer> tile, String model,
            Integer resolution, Float compressionQuality, Boolean invertedAxis,
            ArrayList<Integer> channels, ArrayList<Integer[] > windows,
            ArrayList<String> colors)
    {
        log.info("Setting up handler");
        this.imageId = imageId;
        this.z = z;
        this.t = t;
        this.region = region;
        this.tile = tile;
        this.model = model;
        this.resolution = resolution;
        this.compressionQuality = compressionQuality;
        this.invertedAxis = invertedAxis;
        this.channels = channels;
        this.windows = windows;
        this.colors = colors;
    }

    /**
     * Render Image region event handler. Responds with a <code>image/jpeg</code>
     * body on success based on the <code>longestSide</code> and
     * <code>imageId</code> encoded in the URL or HTTP 404 if the {@link Image}
     * does not exist or the user does not have permissions to access it.
     * @param event Current routing context.
     */
    public byte[] renderImageRegion(omero.client client) {
        try {
            Image image = getImage(client, imageId);
            if (image != null) {
                return getThumbnail(client, image, 96);
            } else {
                log.debug("Cannot find Image:{}", imageId);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving thumbnail", e);
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
            throws ServerError
    {
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
            throws ServerError
    {
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
     * Retrieves a single JPEG thumbnail from the server.
     * @param client OMERO client to use for thumbnail retrieval.
     * @param image {@link Image} instance to retrieve thumbnail for.
     * @param longestSide Size to confine or upscale the longest side of the
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @return JPEG thumbnail as a byte array.
     * @throws Exception
     */
    private byte[] getThumbnail(
            omero.client client, Image image, int longestSide)
            throws Exception
    {
        return getThumbnails(client, Arrays.asList(image), longestSide);
    }

    /**
     * Retrieves a map of JPEG thumbnails from the server.
     * @param client OMERO client to use for thumbnail retrieval.
     * @param images {@link Image} list to retrieve thumbnails for.
     * @param longestSide Size to confine or upscale the longest side of each
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @return Map of primary {@link Pixels} to JPEG thumbnail byte array.
     * @throws Exception
     */
    private byte[] getThumbnails(
            omero.client client, List<? extends IObject> images,
            int longestSide) throws Exception
    {
        log.debug("Getting image region");
        Image image = (Image) images.get(0);
        Integer sizeC = (Integer) unwrap(image.getPrimaryPixels().getSizeC());
        Long pixelsId = (Long) unwrap(image.getPrimaryPixels().getId());
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put(
            "omero.group",
            String.valueOf(unwrap(image.getDetails().getGroup().getId()))
        );
        RenderingEnginePrx renderingEngine =
                client.getSession().createRenderingEngine();
        try {
            // Assume all the groups are the same

            renderingEngine.lookupPixels(pixelsId, ctx);
            if (!(renderingEngine.lookupRenderingDef(pixelsId, ctx))) {
                renderingEngine.resetDefaultSettings(true, ctx);
                renderingEngine.lookupRenderingDef(pixelsId, ctx);
            }
            renderingEngine.load(ctx);
            PlaneDef pDef = new PlaneDef();
            pDef.z = this.z;
            pDef.t = this.t;
            pDef.region = this.getRegionDef(renderingEngine);
            this.setRenderingModel(renderingEngine);
            this.setActiveChannels(renderingEngine, sizeC, ctx);
            this.setResolutionLevel(renderingEngine);
            this.setCompressionLevel(renderingEngine);
            StopWatch t0 = new Slf4JStopWatch("renderCompressed");
            try {
                return renderingEngine.renderCompressed(pDef);
            } finally {
                t0.stop();
            }
        } finally {
            renderingEngine.close();
        }
    }

    private void setCompressionLevel(RenderingEnginePrx renderingEngine)
            throws ServerError
    {
        if (this.compressionQuality != null) {
            renderingEngine.setCompressionLevel(this.compressionQuality);
        } else {
            renderingEngine.setCompressionLevel(0.9f);
        }
    }

    private RegionDef getRegionDef(RenderingEnginePrx renderingEngine)
            throws Exception
    {
        RegionDef regionDef = new RegionDef();
        if (tile != null) {
            regionDef.width = renderingEngine.getTileSize()[0];
            regionDef.height = renderingEngine.getTileSize()[1];
            regionDef.x = tile.get(0) * regionDef.width;
            regionDef.y = tile.get(1) * regionDef.height;
        } else if (region != null) {
            regionDef.x = region.get(0);
            regionDef.y = region.get(1);
            regionDef.width = region.get(2);
            regionDef.height = region.get(3);
        } else {
            String v = "Tile or region argument required.";
            log.error(v);
            throw new Exception(v);
        }
        return regionDef;
    }

    private void setResolutionLevel(RenderingEnginePrx renderingEngine)
            throws ServerError
    {
        if (this.resolution == null) return;
        Integer numberOfLevels = renderingEngine.getResolutionLevels();
        Integer level = numberOfLevels - resolution - 1;
        log.debug("Setting resolution level to: {}", level);
        renderingEngine.setResolutionLevel(level);
    }

    private void setRenderingModel(RenderingEnginePrx renderingEngine)
            throws ServerError
    {
        for (Object a : renderingEngine.getAvailableModels()) {
            if (this.model.equals(((RenderingModel) a).getValue().getValue()))
            {
                renderingEngine.setModel((RenderingModel) a);
                break;
            }
        }
    }

    private void setActiveChannels(
            RenderingEnginePrx renderingEngine, int sizeC,
            Map<String, String> ctx) throws ServerError
    {
        int idx = 0; // index of windows/colors args
        for (int c = 0; c < sizeC; c++) {
            renderingEngine.setActive(c, this.channels.contains(c + 1), ctx);
            if (!this.channels.contains(c + 1)) {
                if (this.channels.contains(-1 * (c + 1))) {
                    idx += 1;
                }
                continue;
            }
            if (this.windows != null)
            {
                float min = (float) this.windows.get(idx)[0];
                float max = (float) this.windows.get(idx)[1];
                log.debug("Channel: {}, [{}, {}]", c, min, max);
                renderingEngine.setChannelWindow(c, min, max, ctx);
            }
            if (this.colors != null) {
                int[] rgba = this.splitHTMLColor(this.colors.get(idx));
                if (rgba != null) {
                    renderingEngine.setRGBA(
                            c, rgba[0], rgba[1], rgba[2], rgba[3], ctx);
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
     *  @param color:   Characters to split.
     *  @return:        rgba - list of Ints
     */
    private int[] splitHTMLColor(String color) {
        ArrayList<Integer> level1 = new ArrayList<Integer>(
                Arrays.asList(3, 4));
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
            log.error(e.getMessage());
        }
        return null;
    }
}
