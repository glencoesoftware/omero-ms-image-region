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

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;
import ome.model.roi.Mask;

public class ShapeMaskCtx extends OmeroRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskCtx.class);

    public static final String CACHE_KEY_FORMAT =
            "%s:%d:%s";  // Class Name, Object ID, Color String

    /** Shape Id */
    public Long shapeId;

    /** Display color */ 
    public String color;

    /**
     * Constructor for jackson to decode the object from string
     */
    ShapeMaskCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an shape mask.
     * @param omeroSessionKey OMERO session key.
     */
    ShapeMaskCtx(MultiMap params, String omeroSessionKey) {
        this.omeroSessionKey = omeroSessionKey;
        shapeId = Long.parseLong(params.get("shapeId"));
        color = params.get("color");

        log.debug("Shape:{}, color: {}", shapeId, color);
    }

    /**
     * Creates a cache key for the context.
     * @return See above.
     */
    public String cacheKey() {
        return String.format(
                CACHE_KEY_FORMAT, Mask.class.getName(), shapeId, color);
    }
}
