/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
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

import java.awt.image.BufferedImage;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import ome.api.IScale;
import ome.api.local.LocalCompress;
import ome.model.core.Pixels;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import omeis.providers.re.lut.LutProvider;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.Image;
import ucar.ma2.Array;

public class ThumbnailsRequestHandler extends ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailsRequestHandler.class);

    /** Current thumbnail rendering context */
    private final ThumbnailCtx thumbnailCtx;

    /** Image scaling service */
    private final IScale iScale;

    /**
     * Default constructor
     * @param thumbnailCtx ThumbnailCtx object
     * @param renderingUtils Configured RenderingUtils
     * @param compressionSrv Compression service
     * @param families List of families
     * @param renderingModels List of renering models
     * @param lutProvider Lookup table provider
     * @param iScale Scaling service
     * @param ngffUtils Configured NgffUtils
     * @param ngffDir Location (local or cloud) of NGFF files
     * @param longestSide Longest side of the final thumbnail
     * @param imageIds imageIds {@link Image} identifiers to get thumbnails for
     * @param renderingDefId optional specific rendering definition id
     */
    public ThumbnailsRequestHandler(
            ThumbnailCtx thumbnailCtx,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            LocalCompress compressionSrv,
            int maxTileLength,
            PixelsService pixelsService,
            IScale iScale) {
        super(thumbnailCtx,
                families,
                renderingModels,
                lutProvider,
                compressionSrv,
                maxTileLength,
                pixelsService);
        this.thumbnailCtx = thumbnailCtx;
        this.iScale = iScale;
    }

    /**
     * Retrieves a map of JPEG thumbnails from the server.
     * @param client OMERO client to use for querying.
     * @return Map of {@link Image} identifier to JPEG thumbnail byte array.
     */
    public Map<Long, byte[]> renderThumbnails(omero.client client) {
        Map<Long, byte[]> thumbnails = new HashMap<Long, byte[]>();
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("render_thumbnails");
        try {
            span.tag("omero.image_ids", thumbnailCtx.imageIds.toString());
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            long userId = sf.getAdminService().getEventContext().userId;
            Map<Long, Pixels> imagePixels = retrievePixDescription(
                    iQuery, thumbnailCtx.imageIds);
            List<Long> pixelsIds = imagePixels
                    .values()
                    .stream()
                    .map(v -> v.getId())
                    .collect(Collectors.toList());
            List<RenderingDef> renderingDefs = retrieveRenderingDefs(
                    client, userId, pixelsIds);
            for (Long imageId  : thumbnailCtx.imageIds) {
                Pixels pixels = imagePixels.get(imageId);
                byte[] thumbnail = new byte[0];
                if (pixels != null) {
                    RenderingDef renderingDef = selectRenderingDef(
                            renderingDefs, userId, pixels.getId());
                    thumbnail = renderThumbnail(client, pixels, renderingDef);
                    if (thumbnail == null) {
                        thumbnail = new byte[0];
                    }
                } else {
                    log.debug("Cannot find Image:{}", imageId);
                }
                thumbnails.put(imageId, thumbnail);
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while rendering thumbnails", e);
        } finally {
            span.finish();
        }
        return thumbnails;
    }

    /**
     * Renders a JPEG thumbnail.
     * @param client OMERO client to use for querying.
     * @return JPEG thumbnail byte array.
     */
    public byte[] renderThumbnail(omero.client client) {
        try {
            IQueryPrx iQuery = client.getSession().getQueryService();
            long imageId = thumbnailCtx.imageIds.get(0);
            Map<Long, Pixels> imagePixels = retrievePixDescription(
                    iQuery, thumbnailCtx.imageIds);
            Pixels pixels = imagePixels.get(imageId);
            if (pixels != null) {
                RenderingDef renderingDef =
                        getRenderingDef(client, pixels.getId());
                return renderThumbnail(client, pixels, renderingDef);
            }
            log.debug("Cannot find Image:{}", imageId);
        } catch (Exception e) {
            log.error("Exception while rendering thumbnail", e);
        }
        return null;
    }

    /**
     * Renders a JPEG thumbnail.
     * @param client OMERO client to use for querying.
     * @param pixels pixels metadata
     * @param renderingDef rendering settings to use for rendering
     * @return JPEG thumbnail byte array.
     */
    private byte[] renderThumbnail(
            omero.client client, Pixels pixels, RenderingDef renderingDef) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("render_thumbnail");
        try {
            span.tag("omero.image_id", pixels.getImage().getId().toString());
            span.tag("omero.pixels_id", pixels.getId().toString());
            thumbnailCtx.format = "jpeg";
            Array array = render(client, pixels, renderingDef);
            int[] shape = array.getShape();
            BufferedImage image = getBufferedImage(array);
            int longestSide = Arrays.stream(shape).max().getAsInt();
            float scale = (float) thumbnailCtx.longestSide / longestSide;
            return compress(iScale.scaleBufferedImage(image, scale, scale));
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while rendering thumbnail", e);
        } finally {
            span.finish();
        }
        return null;
    }

}
