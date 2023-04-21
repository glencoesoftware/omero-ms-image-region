/*
 * Copyright (C) 2023 Glencoe Software, Inc. All rights reserved.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;


import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import loci.formats.FormatTools;
import ome.io.nio.PixelBuffer;
import ome.model.core.Channel;
import ome.model.core.Pixels;
import ome.util.PixelData;
import omeis.providers.re.metadata.StatsFactory;
import omero.ApiUsageException;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class HistogramRequestHandler {

    private static final org.slf4j.Logger log = LoggerFactory
            .getLogger(HistogramRequestHandler.class);

    /** Histogram Request Context */
    HistogramCtx histogramCtx;

    /** OMERO server pixels service. */
    private PixelsService pixelsService;

    /**
     * Constructor. Populates histogramCtx and pixelsService
     * @param histogramCtx
     * @param pixelsService
     */
    public HistogramRequestHandler(HistogramCtx histogramCtx,
            PixelsService pixelsService) {
        this.histogramCtx = histogramCtx;
        this.pixelsService = pixelsService;
    }

    /**
     * Get the minimum and maximum value of the pixel data
     *
     * @param pd
     *            The {@link PixelData}
     * @param channel
     *            The {@link Channel}
     * @return See above
     */
    private double[] getMinMaxFromPixelData(PixelData pd, Channel channel) {
        double min, max;

        StatsFactory sf = new StatsFactory();
        double[] pixelMinMax = sf.initPixelsRange(channel.getPixels());

        min = pixelMinMax[1];
        max = pixelMinMax[0];

        for (int i = 0; i < pd.size(); i++) {
            min = Math.min(min, pd.getPixelValue(i));
            max = Math.max(max, pd.getPixelValue(i));
        }

        return new double[] { min, max };
    }

    /**
     * Read through the pixel data and produce histogram data reflecting
     * the occurrance of pixel values within each bin
     * @param pd The {@link PixelData} to get the pixel values from
     * @param channel The {@link Channel} to get the pixel values from
     * @param minMax The min and max to values to divide into bins
     * @return {@link JsonArray} containing histogram data
     */
    public JsonArray getHistogramData(PixelData pd,
            double[] minMax) {
        int[] counts = new int[histogramCtx.bins];

        double min = minMax[0];
        double max = minMax[1];

        double range = max - min + 1;
        double binRange = range / histogramCtx.bins;
        for (int i = 0; i < pd.size(); i++) {
            int bin = (int) ((pd.getPixelValue(i) - min) / binRange);
            // if there are more bins than values (binRange < 1) the bin will be offset by -1.
            // e.g. min=0.0, max=127.0, binCount=256: a pixel with max value 127.0 would go
            // into bin 254 (expected: 255). Therefore increment by one for these cases.
            if (bin > 0 && binRange < 1)
                bin++;

            if (bin >= 0 && bin < histogramCtx.bins) {
                counts[bin]++;
            } else {
                //Pixel Value outside of min/max range
                throw new IllegalArgumentException(String.format(
                        "Image %d has pixel values %.2f outside of [%.2f, %.2f]",
                        histogramCtx.imageId, pd.getPixelValue(i),
                        minMax[0], minMax[1]));
            }
        }
        JsonArray histogramArray = new JsonArray();
        for (int i : counts) {
            histogramArray.add(i);
        }
        return histogramArray;
    }

    /**
     * Retrieves JSON data representing the histogram for the parameters
     * specified in the histogramCtx
     * @param client
     * @return VertX {@link JsonObject} with the histogram data,
     * the min and max, and whether it was retrieved from StatsInfo
     */
    public JsonObject getHistogramJson(omero.client client) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_histogram");
        JsonObject retVal = new JsonObject();
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            Map<Long, Pixels> imagePixels = retrievePixDescription(
                    iQuery, Arrays.asList(histogramCtx.imageId));
            Pixels pixels = imagePixels.get(histogramCtx.imageId);
            if (pixels == null ||
                   histogramCtx.z >= pixels.getSizeZ() ||
                   histogramCtx.t >= pixels.getSizeT()) {
                return null;
            }
            Channel channel = pixels.getChannel(histogramCtx.c);
            try(PixelBuffer pb = getPixelBuffer(pixels)) {
                //Find resolution level closest to max plane size without
                //exceeding it
                int resolutionLevel = -1;
                for (int i = 0; i < pb.getResolutionLevels(); i++) {
                    pb.setResolutionLevel(i);
                    if (pb.getSizeX() > histogramCtx.maxPlaneWidth ||
                            pb.getSizeY() > histogramCtx.maxPlaneHeight) {
                        break;
                    }
                    resolutionLevel = i;
                }
                if (resolutionLevel < 0) {
                    //No resolution levels exist smaller than max plane size
                    throw new IllegalArgumentException("All resolution levels larger "
                            + "than max plane size");
                }
                pb.setResolutionLevel(resolutionLevel);
                PixelData pd = pb.getPlane(histogramCtx.z, histogramCtx.c,
                                           histogramCtx.t);
                JsonArray histogramArray = new JsonArray();
                double[] minMax = null;
                if (histogramCtx.usePixelsTypeRange) {
                    int bfPixelsType = FormatTools.pixelTypeFromString(
                            pixels.getPixelsType().getValue());
                    long[] minMaxLong = FormatTools.defaultMinMax(bfPixelsType);
                    minMax = new double[] {minMaxLong[0], minMaxLong[1]};
                } else {
                    minMax = getMinMaxFromPixelData(pd, channel);
                }
                histogramArray = getHistogramData(pd, minMax);
                retVal.put("min", minMax[0]);
                retVal.put("max", minMax[1]);
                retVal.put("data", histogramArray);
            }
        } catch (IllegalArgumentException e) {
            span.error(e);
            throw e;
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while retrieving histogram", e);
        } finally {
            span.finish();
        }
        return retVal;
    }

    /**
     * Get Pixels information from Image IDs
     * @param imageIds Image IDs to get Pixels information for
     * @param iQuery Query proxy service
     * @return Map of Image ID vs. Populated Pixels object
     * @throws ApiUsageException
     * @throws ServerError
     */
    protected Map<Long, Pixels> retrievePixDescription(
            IQueryPrx iQuery, List<Long> imageIds)
                throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_pix_description");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_ids", imageIds.toString());
            // Query pulled from ome.logic.PixelsImpl and expanded to include
            // our required Image / Plate metadata; loading both sides of the
            // Image <--> WellSample <--> Well collection so that we can
            // resolve our field index.
            ParametersI params = new ParametersI();
            params.addIds(imageIds);
            List<Pixels> pixelsList = (List<Pixels>) new IceMapper().reverse(
                    iQuery.findAllByQuery(
                        "select p from Pixels as p "
                        + "join fetch p.image as i "
                        + "left outer join fetch i.details.externalInfo "
                        + "join fetch p.pixelsType "
                        + "join fetch p.channels as c "
                        + "join fetch c.logicalChannel as lc "
                        + "left outer join fetch c.statsInfo "
                        + "where i.id in (:ids)", params, ctx));
            Map<Long, Pixels> toReturn = new HashMap<Long, Pixels>();
            for (Pixels pixels : pixelsList) {
                toReturn.put(pixels.getImage().getId(), pixels);
            }
            return toReturn;
        } finally {
            span.finish();
        }
    }

    /**
     * Returns a pixel buffer for a given set of pixels.
     * @param pixels pixels metadata
     * @return See above.
     * @see PixelsService#getPixelBuffer(Pixels)
     */
    private PixelBuffer getPixelBuffer(Pixels pixels) {
        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan("get_pixel_buffer");
        try {
            span.tag("omero.pixels_id", Long.toString(pixels.getId()));
            return pixelsService.getPixelBuffer(pixels, false);
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    /**
     * Whether or not a single {@link MaskI} can be read from the server.
     * @param client OMERO client to use for querying.
     * @return <code>true</code> if the {@link Mask} can be loaded or
     * <code>false</code> otherwise.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    public boolean canRead(omero.client client) {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(histogramCtx.imageId);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("can_read");
        try {
            List<List<RType>> rows = client.getSession()
                    .getQueryService().projection(
                            "SELECT i.id FROM Image as i " +
                            "WHERE i.id = :id", params, ctx);
            if (rows.size() > 0) {
                return true;
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while checking histogram readability", e);
        } finally {
            span.finish();
        }
        return false;
    }

}
