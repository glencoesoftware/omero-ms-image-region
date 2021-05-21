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
import java.net.URI;
import java.nio.file.Paths;

import org.slf4j.LoggerFactory;

import ome.api.IQuery;
import ome.io.nio.BackOff;
import ome.io.nio.FilePathResolver;
import ome.io.nio.PixelBuffer;
import ome.io.nio.TileSizes;
import ome.model.core.Pixels;
/**
 * Subclass which overrides series retrieval to avoid the need for
 * an injected {@link IQuery}.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelsService extends ome.io.nio.PixelsService {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    /** NGFF directory root */
    private final URI ngffDir;

    /** Max Tile Length */
    private final Integer maxTileLength;

    public PixelsService(
            String path, long memoizerWait, FilePathResolver resolver,
            BackOff backOff, TileSizes sizes, IQuery iQuery, String ngffDir,
            Integer maxTileLength) {
        super(
            path, true, new File(new File(path), "BioFormatsCache"),
            memoizerWait, resolver, backOff, sizes, iQuery);
        this.ngffDir = asUri(ngffDir);
        this.maxTileLength = maxTileLength;
        log.info("Using image region PixelsService");
    }

    private URI asUri(String ngffDir) {
        try {
            URI asUri = new URI(ngffDir);
            if ("s3".equals(this.ngffDir.getScheme())) {
                return asUri;
            }
        } catch (Exception e) {
            // Fall through
        }
        return Paths.get(ngffDir).toUri();
    }

    @Override
    protected int getSeries(Pixels pixels) {
        return pixels.getImage().getSeries();
    }

    /**
     * Returns a pixel buffer for a given set of pixels. Either n NGFF pixel
     * buffer, a proprietary ROMIO pixel buffer or a specific pixel buffer
     * implementation.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return A pixel buffer instance.
     */
    public PixelBuffer getPixelBuffer(Pixels pixels, boolean write) {
        if (!ngffDir.getPath().isEmpty()) {
            try {
                String subPath = String.format(
                        "/%d.zarr/%d",
                        pixels.getImage().getFileset().getId(),
                        pixels.getImage().getSeries());
                return new ZarrPixelBuffer(
                        pixels, ngffDir.resolve(ngffDir.getPath() + subPath),
                        maxTileLength);
            } catch (Exception e) {
                log.info(
                    "Getting NGFF Pixel Buffer failed - " +
                    "attempting to get local data", e);
            }
        }
        return _getPixelBuffer(pixels, write);
    }

}

