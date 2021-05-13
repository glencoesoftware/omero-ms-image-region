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
import java.io.IOException;
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
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.util.ImageUtil;
import omeis.providers.re.lut.LutProvider;
import omero.RType;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.Image;
import omero.sys.ParametersI;
import ucar.ma2.Array;
import omeis.providers.re.codomain.ReverseIntensityContext;

public class ThumbnailsRequestHandler extends ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailsRequestHandler.class);

    /** Current thumbanil rendering context */
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
            String ngffDir,
            OmeroZarrUtils zarrUtils,
            IScale iScale) {
        super(thumbnailCtx,
                families,
                renderingModels,
                lutProvider,
                compressionSrv,
                maxTileLength,
                pixelsService,
                ngffDir,
                zarrUtils);
        this.thumbnailCtx = thumbnailCtx;
        this.iScale = iScale;
    }

    /**
     * Retrieves a map of JPEG thumbnails from the server.
     * @return Map of {@link Image} identifier to JPEG thumbnail byte array.
     */
    public Map<Long, byte[]> renderThumbnails(omero.client client) {
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            IPixelsPrx iPixels = sf.getPixelsService();
            List<Image> images = getImages(client, thumbnailCtx.imageIds);
            if (images.size() != 0) {
                return getThumbnails(iQuery, iPixels, images);
            } else {
                log.debug("Cannot find any Images with Ids {}",
                        thumbnailCtx.imageIds);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving thumbnails", e);
        }
        return null;
    }

    /**
     * Retrieves a list of loaded {@link Image}s from the server.
     * @param client OMERO client to use for querying.
     * @param imageIds {@link Image} identifiers to query for.
     * @return List of loaded {@link Image} and primary {@link Pixels}.
     * @throws ServerError If there was any sort of error retrieving the images.
     */
    protected List<Image> getImages(omero.client client, List<Long> imageIds)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addIds(imageIds);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_images");
        try {
            return client.getSession().getQueryService().findAllByQuery(
                "SELECT i FROM Image as i " +
                "JOIN FETCH i.pixels as p WHERE i.id IN (:ids)",
                params, ctx
            ).stream().map(x -> (Image) x).collect(Collectors.toList());
        } finally {
            span.finish();
        }
    }

    /**
     * Retrieves a byte array of rendered pixel data for the thumbnail
     * @param iQuery The query service proxy
     * @param iPixels The pixels service proxy
     * @param image The Image the use wants a thumbnail of
     * @param longestSide The longest side length of the final thumbnail
     * @return Byte array of jpeg thumbnail data
     * @throws IOException
     */
    protected byte[] getThumbnail(
        IQueryPrx iQuery, IPixelsPrx iPixels, Image image)
            throws IOException {
        try {
            List<RType> pixelsIdAndSeries = getPixelsIdAndSeries(
                iQuery, image.getId().getValue());
            return getRegion(iQuery, iPixels, pixelsIdAndSeries);
        } catch (Exception e) {
            log.error("Error getting thumbnail {}", Long.toString(image.getId().getValue()), e);
            return new byte[0];
        }
    }

    /**
     * Retrieves a map of JPEG thumbnails from ngffDir.
     * @param images {@link Image} list to retrieve thumbnails for.
     * @param longestSide Size to confine or upscale the longest side of each
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @return Map of {@link Image} identifier to JPEG thumbnail byte array.
     * @throws IOException
     */
    protected Map<Long, byte[]> getThumbnails(
        IQueryPrx iQuery, IPixelsPrx iPixels, List<Image> images)
            throws IOException {
        Map<Long, byte[]> thumbnails = new HashMap<Long, byte[]>();
        for (Image image : images) {
            thumbnails.put(
                    image.getId().getValue(),
                    getThumbnail(iQuery, iPixels, image));
        }
        return thumbnails;
    }

    /**
     * Retrieves a JPEG thumbnail from the server.
     * @return JPEG thumbnail byte array.
     */
    public byte[] renderThumbnail(omero.client client) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("render_image_region");
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            IPixelsPrx iPixels = sf.getPixelsService();
            List<RType> pixelsIdAndSeries = getPixelsIdAndSeries(
                    iQuery, thumbnailCtx.imageIds.get(0));
            thumbnailCtx.format = "jpeg";
            if (pixelsIdAndSeries != null && pixelsIdAndSeries.size() == 2) {
                Pixels pixels = retrievePixDescription(
                        pixelsIdAndSeries, mapper, iPixels, iQuery);
                Array array = render(pixels, iPixels);
                int[] shape = array.getShape();
                BufferedImage image = ImageUtil.createBufferedImage(
                        (int[]) array.getStorage(), shape[1], shape[0]);
                int longestSide = Arrays.stream(shape).max().getAsInt();
                float scale = (float) thumbnailCtx.longestSide / longestSide;
                return compress(iScale.scaleBufferedImage(image, scale, scale));
            }
            log.debug("Cannot find Image:{}", thumbnailCtx.imageIds.get(0));
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while retrieving image region", e);
        } finally {
            span.finish();
        }
        return null;
    }

}
