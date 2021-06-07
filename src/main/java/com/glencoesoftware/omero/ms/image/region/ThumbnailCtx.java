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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import io.vertx.core.MultiMap;
import ome.io.nio.PixelBuffer;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import omeis.providers.re.Renderer;

public class ThumbnailCtx extends ImageRegionCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailCtx.class);

    /** The size of the longest side of the thumbnail */
    public Integer longestSide;

    /** Image IDs to get a thumbnail for */
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

        imageIds = new ArrayList<Long>();
        imageIds.addAll(
                params.getAll("imageId").stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList()));
        imageIds.addAll(
                params.getAll("id").stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList()));

        this.renderingDefId = Optional.ofNullable(params.get("rdefId"))
                .map(Long::parseLong).orElse(null);
    }

    /**
     * Apply the first resolution level larger than the thumbnail
     * @param resolutionDescriptions
     * @return
     */
    @Override
    public void setResolutionLevel(
            Renderer renderer, PixelBuffer pixelBuffer) {
        List<List<Integer>> rds = pixelBuffer.getResolutionDescriptions();

        int resolutionLevel = rds.size() - 1;
        for (; resolutionLevel >= 0; resolutionLevel--) {
            if (rds.get(resolutionLevel).get(0) < longestSide
                && rds.get(resolutionLevel).get(1) < longestSide) {
                break;
            }
        }
        resolutionLevel += 1;
        if (resolutionLevel < 0) {
            throw new IllegalArgumentException(
                    "longestSide exceeds image size");
        }
        log.debug("Selected resolution level: {}", resolutionLevel);
        renderer.setResolutionLevel(resolutionLevel);
    }

    @Override
    public void updateSettings(Renderer renderer, List<Family> families,
            List<RenderingModel> renderingModels) {
        // No-op for thumbnails as we are always taking our settings from
        // the current RenderingDef
    }
}
