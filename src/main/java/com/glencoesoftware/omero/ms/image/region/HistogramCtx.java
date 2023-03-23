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


import org.slf4j.LoggerFactory;

import io.vertx.core.MultiMap;

public class HistogramCtx extends MicroserviceRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(HistogramCtx.class);

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


    /**
     * Constructor for jackson to decode the object from string
     */
    HistogramCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     * @param omeroSessionKey OMERO session key.
     */
    HistogramCtx(MultiMap params, String omeroSessionKey) {
        this.omeroSessionKey = omeroSessionKey;
        imageId = getImageIdFromString(getCheckedParam(params, "imageId"));
        c = getIntegerFromString(getCheckedParam(params, "theC"));
        z = getIntegerFromString(getCheckedParam(params, "z"));
        t = getIntegerFromString(getCheckedParam(params, "t"));
        maxPlaneWidth = getIntegerFromString(getCheckedParam(params, "maxPlaneWidth"));
        maxPlaneHeight = getIntegerFromString(getCheckedParam(params, "maxPlaneHeight"));
        bins = params.get("bins") == null ? 256 : getIntegerFromString(params.get("bins"));
    }
}
