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
import ome.model.core.Pixels;
import ome.util.PixelData;
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

    public JsonObject getHistogramJson2(omero.client client) {
        JsonObject retObj = new JsonObject();
        retObj.put("TestKey", "TestVal");
        return retObj;
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
                PixelData pd = pb.getPlane(histogramCtx.z, histogramCtx.c, histogramCtx.t);

                //Calculate bin ranges
                int[] counts = new int[histogramCtx.bins];
                double binFactor = (double) histogramCtx.bins / Math.pow(256, pd.bytesPerPixel());
                for (int i = 0; i < pd.size(); i++) {
                    double val = pd.getPixelValue(i);
                    int binVal = (int) Math.floor(val * binFactor);
                    counts[binVal] += 1;
                }
                JsonArray histogramArray = new JsonArray();
                for (int i : counts) {
                    histogramArray.add(i);
                }
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
