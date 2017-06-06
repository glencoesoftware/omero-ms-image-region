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

import static omero.rtypes.rint;
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
import omero.api.ThumbnailStorePrx;
import omero.model.IObject;
import omero.model.Image;
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

    /**
     * Default constructor.
     * @param z Index of the z section to render the region for.
     * @param t Index of the time point to render the region for.
     */
    public ImageRegionRequestHandler(Long imageId, int z, int t) {
        this.imageId = imageId;
        this.z = z;
        this.t = t;
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
        return getThumbnails(client, Arrays.asList(image), longestSide)
                .get(unwrap(image.getPrimaryPixels().getId()));
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
    private Map<Long, byte[]> getThumbnails(
            omero.client client, List<? extends IObject> images,
            int longestSide)
                    throws ServerError{
        ThumbnailStorePrx thumbnailStore =
                client.getSession().createThumbnailStore();
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            List<Long> pixelsIds = new ArrayList<Long>();
            for (IObject o : images) {
                Image image = (Image) o;
                pixelsIds.add((Long) unwrap(image.getPrimaryPixels().getId()));
                // Assume all the groups are the same
                ctx.put(
                    "omero.group",
                    String.valueOf(unwrap(
                            image.getDetails().getGroup().getId()))
                );
            }
            StopWatch t0 = new Slf4JStopWatch("getThumbnailByLongestSideSet");
            try {
                return thumbnailStore.getThumbnailByLongestSideSet(
                    rint(longestSide), pixelsIds, ctx
                );
            } finally {
                t0.stop();
            }
        } finally {
            thumbnailStore.close();
        }
    }
}
