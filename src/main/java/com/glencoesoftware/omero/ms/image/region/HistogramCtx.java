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


import org.slf4j.LoggerFactory;

import io.vertx.core.MultiMap;

public class HistogramCtx extends MicroserviceRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(HistogramCtx.class);

    public static final String CACHE_KEY_FORMAT =
            "%d:%d:%d:%d:%d";  // ImageId, c, z, t, bins

    /** Image ID */
    public Long imageId;

    /** c - channel index */
    public Integer c;

    /** Number of bins in the histogram */
    public Integer bins;

    /** z - index */
    public Integer z;

    /** t - index */
    public Integer t;

    /** Max Plane Width */
    public Integer maxPlaneWidth;

    /** Max Plane Height */
    public Integer maxPlaneHeight;

    /** Set histogram min and max to the pixels type min and max rather than
     * the min and max values for the actual pixel values
     */
    public boolean usePixelsTypeRange = false;


    /**
     * Constructor for jackson to decode the object from string
     */
    HistogramCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for retrieving the histogram.
     * @param omeroSessionKey OMERO session key.
     */
    HistogramCtx(MultiMap params, String omeroSessionKey) {
        this.omeroSessionKey = omeroSessionKey;
        imageId = getImageIdFromString(getCheckedParam(params, "imageId"));
        c = getIntegerFromString(getCheckedParam(params, "theC"));
        String zStr = params.get("z") == null ? params.get("theZ") : params.get("z");
        if (zStr == null) {
            throw new IllegalArgumentException("Must provide either 'theZ' or "
                    + "'z' parameter");
        }
        z = getIntegerFromString(zStr);
        String tStr = params.get("t") == null ? params.get("theT") : params.get("t");
        if (tStr == null) {
            throw new IllegalArgumentException("Must provide either 'theT' or "
                    + "'t' parameter");
        }
        t = getIntegerFromString(tStr);
        maxPlaneWidth = getIntegerFromString(getCheckedParam(params, "maxPlaneWidth"));
        maxPlaneHeight = getIntegerFromString(getCheckedParam(params, "maxPlaneHeight"));
        bins = params.get("bins") == null ? 256 : getIntegerFromString(params.get("bins"));
        usePixelsTypeRange = getBooleanParameter(params, "usePixelsTypeRange");
    }

    /**
     * Creates a cache key for the context.
     * @return See above.
     */
    public String cacheKey() {
        return String.format(
                CACHE_KEY_FORMAT, imageId, c, z, t, bins);
    }
}
