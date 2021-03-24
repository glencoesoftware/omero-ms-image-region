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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;

public class ThumbnailCtx extends OmeroRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailCtx.class);

    /** The size of the longest side of the thumbnail */
    public Integer longestSide;

    /** Image ID */
    public Long imageId;

    /** Image IDs */
    public List<Long> imageIds;

    /** Rendering Definition ID */
    public Long renderingDefId;

    /**
     * Constructor for jackson to decode the object from string
     */
    ThumbnailCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     * @param omeroSessionKey OMERO session key.
     */
    ThumbnailCtx(MultiMap params, String omeroSessionKey) {
        this.omeroSessionKey = omeroSessionKey;

        this.longestSide = Optional.ofNullable(params.get("longestSide"))
                .map(Integer::parseInt)
                .orElse(96);

        this.imageIds = params.getAll("id").stream()
                .map(Long::parseLong)
                .collect(Collectors.toList());

        this.imageId = Optional.ofNullable(params.get("imageId"))
                .map(Long::parseLong)
                .orElse(null);

        this.renderingDefId = Optional.ofNullable(params.get("rdefId"))
                .map(Long::parseLong).orElse(null);

    }
}
