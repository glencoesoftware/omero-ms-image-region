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

import java.io.File;

import ome.api.IQuery;
import ome.io.nio.BackOff;
import ome.io.nio.FilePathResolver;
import ome.io.nio.TileSizes;
import ome.model.core.Pixels;


/**
 * Subclass which overrides series retrieval to avoid the need for
 * an injected {@link IQuery}.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelsService extends com.glencoesoftware.omero.ms.core.PixelsService {

    public PixelsService(
            String path, boolean isReadOnlyRepo, File memoizerDirectory,
            long memoizerWait, FilePathResolver resolver, BackOff backOff,
            TileSizes sizes, IQuery iQuery,
            long zarrCacheSize,
            int maxPlaneWidth, int maxPlaneHeight) {
        super(
            path, isReadOnlyRepo, memoizerDirectory, memoizerWait, resolver,
            backOff, sizes, iQuery, zarrCacheSize, maxPlaneWidth, maxPlaneHeight);
    }

    @Override
    protected int getSeries(Pixels pixels) {
        return pixels.getImage().getSeries();
    }

}