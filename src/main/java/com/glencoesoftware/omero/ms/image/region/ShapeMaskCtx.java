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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;
import ome.model.roi.Mask;
import ome.xml.model.primitives.Color;
import omeis.providers.re.data.RegionDef;

public class ShapeMaskCtx extends OmeroRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskCtx.class);

    public static final String CACHE_KEY_FORMAT =
            "%s:%d:%s";  // Class Name, Object ID, Color String

    /** Shape Id */
    public Long shapeId;

    /** Display color */
    public Color color;

    /** Whether or not to flip horizontally */
    public boolean flipHorizontal;

    /** Whether or not to flip vertically */
    public boolean flipVertical;

    public RegionDef region;

    public RegionDef tile;

    /** Resolution to read */
    public Integer resolution;

    /** Subarray Domain String for Label Images */
    public String subarrayDomainStr;

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
        try {
            this.omeroSessionKey = omeroSessionKey;
            shapeId = Long.parseLong(params.get("shapeId"));
            color = splitHTMLColor(params.get("color"));
            String flip = Optional.ofNullable(params.get("flip"))
                    .orElse("").toLowerCase();
            flipHorizontal = flip.contains("h");
            flipVertical = flip.contains("v");
            if(params.get("resolution") != null) {
                resolution = Integer.valueOf(params.get("resolution"));
            }
            getTileFromString(params.get("tile"));
            subarrayDomainStr = params.get("domain");
        } catch (Exception e) {
            log.error("Error creating ShapeMaskCtx", e);
            throw e;
        }
    }

    /**
     * Parse a string to RegionDef and Int describing tile and resolution.
     * @param tileString string describing the tile to render:
     * "1,1,0,1024,1024"
     */
    private void getTileFromString(String tileString) {
        if (tileString == null) {
            return;
        }
        String[] tileArray = tileString.split(",", -1);
        tile = new RegionDef();
        tile.setX(Integer.parseInt(tileArray[1]));
        tile.setY(Integer.parseInt(tileArray[2]));
        if (tileArray.length == 5) {
            tile.setWidth(Integer.parseInt(tileArray[3]));
            tile.setHeight(Integer.parseInt(tileArray[4]));
        }
        resolution = Integer.parseInt(tileArray[0]);
    }

    /**
     * Creates a cache key for the context.
     * @return See above.
     */
    public String cacheKey() {
        return String.format(
                CACHE_KEY_FORMAT, Mask.class.getName(), shapeId, color);
    }

    /**
     *  Splits an hex stream of characters into an array of bytes
     *  in format (R,G,B,A) and converts to a
     *  {@link ome.xml.model.primitives.Color}.
     *  - abc      -> (0xAA, 0xBB, 0xCC, 0xFF)
     *  - abcd     -> (0xAA, 0xBB, 0xCC, 0xDD)
     *  - abbccd   -> (0xAB, 0xBC, 0xCD, 0xFF)
     *  - abbccdde -> (0xAB, 0xBC, 0xCD, 0xDE)
     *  @param color Characters to split.
     *  @return corresponding {@link ome.xml.model.primitives.Color}
     */
    public static Color splitHTMLColor(String color) {
        List<Integer> level1 = Arrays.asList(3, 4);
        try {
            if (level1.contains(color.length())) {
                String c = color;
                color = "";
                for (char ch : c.toCharArray()) {
                    color += ch + ch;
                }
            }
            if (color.length() == 6) {
                color += "FF";
            }
            if (color.length() == 8) {
                int r = Integer.parseInt(color.substring(0, 2), 16);
                int g = Integer.parseInt(color.substring(2, 4), 16);
                int b = Integer.parseInt(color.substring(4, 6), 16);
                int a = Integer.parseInt(color.substring(6, 8), 16);
                return new Color(r, g, b, a);
            }
        } catch (Exception e) {
            log.error("Error while parsing color: {}", color, e);
        }
        return null;
    }
}
