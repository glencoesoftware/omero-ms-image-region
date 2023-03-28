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
import ome.io.nio.PixelBuffer;
import ome.model.core.Channel;
import ome.model.core.Pixels;
import ome.model.enums.PixelsType;
import ome.util.PixelData;
import omeis.providers.re.metadata.StatsFactory;
import omero.ApiUsageException;
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

    public HistogramRequestHandler(HistogramCtx histogramCtx,
            PixelsService pixelsService) {
        this.histogramCtx = histogramCtx;
        this.pixelsService = pixelsService;
    }

    private double[] getMaxValueForPixelsType(PixelsType pt) {
        if (pt.getValue().equals(PixelsType.VALUE_BIT)) {
            return new double[] {0, 1};
        } else if (pt.getValue().equals(PixelsType.VALUE_UINT8)) {
            return new double[] {0, 255};
        } else if (pt.getValue().equals(PixelsType.VALUE_INT8)) {
            return new double[] {-128, 127};
        } else if (pt.getValue().equals(PixelsType.VALUE_UINT16)) {
            return new double[] {0, 256*256 - 1};
        } else if (pt.getValue().equals(PixelsType.VALUE_INT16)) {
            return new double[] {-256*128, 256*128 - 1};
        } else if (pt.getValue().equals(PixelsType.VALUE_UINT32)) {
            return new double[] {0, 256*256*256*256 - 1};
        } else if (pt.getValue().equals(PixelsType.VALUE_INT32)) {
            return new double[] {-256*256*256*128, 256*256*256*128 - 1};
        } else if (pt.getValue().equals(PixelsType.VALUE_FLOAT)) {
            return new double[] {-Float.MAX_VALUE, Float.MAX_VALUE};
        } else if (pt.getValue().equals(PixelsType.VALUE_DOUBLE)) {
            return new double[] {-Double.MAX_VALUE, Double.MAX_VALUE};
        }
        throw new IllegalArgumentException("Unsupported PixelsType: "
                                           + pt.getValue());
    }

    /**
     * Get the minimum and maximum value to use for the histogram. If useGlobal
     * is <code>true</code> and the channel has stats calculated the global
     * minimum and maximum will be used, otherwise the minimum and maximum value
     * of the plane will be used.
     *
     * @param pd
     *            The {@link PixelData}
     * @param channel
     *            The {@link Channel}
     * @param useGlobal
     *            Try to use the global minimum/maximum
     * @return See above
     */
    private double[] determineHistogramMinMax(PixelData pd, Channel channel,
            boolean useGlobal) {
        double min, max;

        if (useGlobal && channel.getStatsInfo() != null) {
            min = channel.getStatsInfo().getGlobalMin();
            max = channel.getStatsInfo().getGlobalMax();
            // if max == 1.0 the global min/max probably has not been
            // calculated; fall back to plane min/max
            if (max != 1.0)
                return new double[] { min, max };
        }

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

    private JsonArray getHistogramData(PixelData pd, Channel channel,
            double[] minMax, int imgWidth, int imgHeight) {
        int[] counts = new int[histogramCtx.bins];

        double min = minMax[0];
        double max = minMax[1];

        double range = max - min + 1;
        double binRange = range / histogramCtx.bins;
        for (int i = 0; i < pd.size(); i++) {
            int pdx = i % imgWidth;
            int pdy = i / imgWidth;
            if (pdx >= 0 && pdx < (0 + imgWidth) && pdy >= 0 && pdy < (0 + imgHeight)) {
                double scaledValue = (pd.getPixelValue(i) - min);
                int bin = (int) ((pd.getPixelValue(i) - min) / binRange);
                // if there are more bins than values (binRange < 1) the bin will be offset by -1.
                // e.g. min=0.0, max=127.0, binCount=256: a pixel with max value 127.0 would go
                // into bin 254 (expected: 255). Therefore increment by one for these cases.
                if (bin > 0 && binRange < 1)
                    bin++;

                if (bin >= 0 && bin < histogramCtx.bins)
                    counts[bin]++;
            }
        }
        JsonArray histogramArray = new JsonArray();
        for (int i : counts) {
            histogramArray.add(i);
        }
        return histogramArray;
    }

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
            Channel channel = pixels.getChannel(histogramCtx.c);
            try(PixelBuffer pb = getPixelBuffer(pixels)) {
                //Find resolution level closest to max plane size without
                //exceeding it
                int resolutionLevel = 0;
                for (int i = 1; i < pb.getResolutionLevels(); i++) {
                    pb.setResolutionLevel(i);
                    if (pb.getSizeX() > histogramCtx.maxPlaneWidth ||
                            pb.getSizeY() > histogramCtx.maxPlaneHeight) {
                        break;
                    }
                    resolutionLevel = i;
                }
                pb.setResolutionLevel(resolutionLevel);
                PixelData pd = pb.getPlane(histogramCtx.z, histogramCtx.c,
                                           histogramCtx.t);
                JsonArray histogramArray = new JsonArray();
                double[] minMax = null;
                if (histogramCtx.useChannelRange) {
                    //TODO: Support useGlobal?
                    minMax = determineHistogramMinMax(pd, channel, false);
                } else {
                    minMax = getMaxValueForPixelsType(pixels.getPixelsType());
                }
                histogramArray = getHistogramData(pd, channel, minMax,
                        pb.getSizeX(),
                        pb.getSizeY());
                retVal.put("data", histogramArray);
            }
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
                        + "left outer join fetch i.wellSamples as ws "
                        + "left outer join fetch ws.well as w "
                        + "left outer join fetch w.wellSamples "
                        + "join fetch p.pixelsType "
                        + "join fetch p.channels as c "
                        + "join fetch c.logicalChannel as lc "
                        + "left outer join fetch c.statsInfo "
                        + "left outer join fetch lc.photometricInterpretation "
                        + "left outer join fetch lc.illumination "
                        + "left outer join fetch lc.mode "
                        + "left outer join fetch lc.contrastMethod "
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

}
