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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import ome.api.local.LocalCompress;
import ome.io.nio.PixelBuffer;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.model.fs.Fileset;
import ome.xml.model.primitives.Color;
import omeis.providers.re.Renderer;
import omeis.providers.re.codomain.ReverseIntensityContext;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omero.ApiUsageException;
import omero.RType;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public abstract class OmeroVertxRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(OmeroVertxRequestHandler.class);

    public OmeroVertxRequestHandler() {

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
