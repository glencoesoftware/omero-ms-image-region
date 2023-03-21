package com.glencoesoftware.omero.ms.image.region;

import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

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

    public JsonObject getHistogramJson(omero.client client) {
        JsonObject retObj = new JsonObject();
        retObj.put("TestKey", "TestVal");
        return retObj;
    }
}
