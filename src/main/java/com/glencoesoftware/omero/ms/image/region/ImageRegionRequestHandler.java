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
    private final int resolution;

    /** Region to read */
    private final RegionDef region;

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
            Long imageId, int z, int t, RegionDef region, int resolution,
            ArrayList<Integer> channels, ArrayList<Integer[] > windows,
            ArrayList<String> colors)
    {
        this.imageId = imageId;
        this.z = z;
        this.t = t;
        this.region = region;
        this.resolution = resolution;
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
     * Retrieves a single JPEG thumbnail from the server.
     * @param client OMERO client to use for thumbnail retrieval.
     * @param image {@link Image} instance to retrieve thumbnail for.
     * @param longestSide Size to confine or upscale the longest side of the
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @return JPEG thumbnail as a byte array.
     * @throws ServerError If there was any sort of error retrieving the
     * thumbnails.
     */
    private byte[] getThumbnail(
            omero.client client, Image image, int longestSide)
                    throws ServerError {
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
     * @throws ServerError If there was any sort of error retrieving the
     * thumbnails.
     */
    private byte[] getThumbnails(
            omero.client client, List<? extends IObject> images,
            int longestSide)
                    throws ServerError{
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
            renderingEngine.setCompressionLevel(0.9f);
            renderingEngine.setResolutionLevel(this.resolution);
            this.setActiveChannels(renderingEngine, sizeC, ctx);
            PlaneDef pDef = new PlaneDef();
            pDef.z = 0;
            pDef.t = 0;
            pDef.region = this.region;
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

    private void setActiveChannels(
            RenderingEnginePrx renderingEngine, int sizeC,
            Map<String, String> ctx) throws ServerError
    {
        int idx = 0; // index of windows/colors args
        for (int c = 0; c < sizeC; c++) {
            renderingEngine.setActive(c, this.channels.contains(c), ctx);
            if (this.windows != null)
            {
                log.debug("{}", this.windows.get(idx).length);
                renderingEngine.setChannelWindow(
                        c, (float) this.windows.get(idx)[0],
                        (float) this.windows.get(idx)[1], ctx);
            }
        }
    }

}
