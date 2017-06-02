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

import omero.model.Image;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    /** z - index. */
    private final int z;

    /** t -index. */
    private final int t;

    /**
     * Default constructor.
     * @param z Index of the z section to render the region for.
     * @param t Index of the time point to render the region for.
     */
    public ImageRegionRequestHandler(int z, int t) {
        this.z = z;
        this.t = t;
    }

    
}
