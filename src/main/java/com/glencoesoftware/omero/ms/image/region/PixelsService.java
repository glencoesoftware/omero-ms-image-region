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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.slf4j.LoggerFactory;

import ome.api.IQuery;
import ome.io.nio.BackOff;
import ome.io.nio.FilePathResolver;
import ome.io.nio.PixelBuffer;
import ome.io.nio.TileSizes;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.screen.Well;
import ome.model.screen.WellSample;

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
    private final Path ngffDir;

    /** Max Tile Length */
    private final Integer maxTileLength;

    public PixelsService(
            String path, long memoizerWait, FilePathResolver resolver,
            BackOff backOff, TileSizes sizes, IQuery iQuery, String ngffDir,
            Integer maxTileLength) throws IOException {
        super(
            path, true, new File(new File(path), "BioFormatsCache"),
            memoizerWait, resolver, backOff, sizes, iQuery);
        this.ngffDir = asPath(ngffDir);
        this.maxTileLength = maxTileLength;
    }

    /**
     * Converts an NGFF root string to a path, initializing a {@link FileSystem}
     * if required
     * @param ngffDir NGFF directory root
     * @return Fully initialized path or <code>null</code> if the NGFF root
     * directory has not been specified in configuration.
     * @throws IOException
     */
    private Path asPath(String ngffDir) throws IOException {
        if (ngffDir.isEmpty()) {
            return null;
        }

        try {
            URI uri = new URI(ngffDir);
            if ("s3".equals(uri.getScheme())) {
                URI endpoint = new URI(
                        uri.getScheme(), uri.getUserInfo(), uri.getHost(),
                        uri.getPort(), "", "", "");
                // drop initial "/"
                String uriPath = uri.getRawPath().substring(1);
                int first = uriPath.indexOf("/");
                String bucket = "/" + uriPath.substring(0, first);
                String rest = uriPath.substring(first + 1);
                // FIXME: We might want to support additional S3FS settings in
                // the future.  See:
                //   * https://github.com/lasersonlab/Amazon-S3-FileSystem-NIO
                FileSystem fs = FileSystems.newFileSystem(endpoint, null);
                return fs.getPath(bucket, rest);
            }
        } catch (URISyntaxException e) {
            // Fall through
        }
        return Paths.get(ngffDir);
    }

    @Override
    protected int getSeries(Pixels pixels) {
        return pixels.getImage().getSeries();
    }

    /**
     * Get the region shape and the start (offset) from the string
     * @param domainStr The string which describes the domain
     * @return 2D int array [[shape_dim1,...],[start_dim1,...]]
     */
    public int[][] getShapeAndStartFromString(String domainStr) {
        //String like [0,1,0,100:150,200:250]
        if (domainStr.length() == 0) {
            return null;
        }
        if (domainStr.startsWith("[")) {
            domainStr = domainStr.substring(1);
        }
        if (domainStr.endsWith("]")) {
            domainStr = domainStr.substring(0, domainStr.length() - 1);
        }
        String[] dimStrs = domainStr.split(",");
        if (dimStrs.length != 5) {
            throw new IllegalArgumentException(
                    "Invalid number of dimensions in domain string");
        }
        int[][] shapeAndStart = new int[][] {new int[5], new int[5]};
        for (int i = 0; i < 5; i++) {
            String s = dimStrs[i];
            if(s.contains(":")) {
                String[] startEnd = s.split(":");
                shapeAndStart[0][i] =
                        Integer.valueOf(startEnd[1]) -
                        Integer.valueOf(startEnd[0]); //shape
                shapeAndStart[1][i] = Integer.valueOf(startEnd[0]); //start
            } else {
                shapeAndStart[0][i] = 1; //shape - size 1 in this dim
                shapeAndStart[1][i] = Integer.valueOf(s); //start
            }
        }
        return shapeAndStart;
    }

    private String getImageSubPath(Pixels pixels) {
        Image image = pixels.getImage();
        Long filesetId = image.getFileset().getId();
        int wellSampleCount = image.sizeOfWellSamples();
        if (wellSampleCount > 0) {
            if (wellSampleCount != 1) {
                throw new IllegalArgumentException(
                        "Cannot resolve Image <--> Well mapping with "
                        + "WellSample count = " + wellSampleCount);
            }
            WellSample ws = image.iterateWellSamples().next();
            Well well = ws.getWell();
            Iterator<WellSample> i = well.iterateWellSamples();
            int field = 0;
            while (i.hasNext()) {
                WellSample v = i.next();
                if (v.getId() == ws.getId()) {
                    break;
                }
                field++;
            }
            return String.format(
                    "%d.zarr/%d/%d/%d",
                    filesetId, well.getRow(), well.getColumn(), field);
        }
        return String.format(
                "%d.zarr/%d/labels/%s", filesetId, image.getSeries());
    }

    private String getLabelImageSubPath(Pixels pixels, String uuid) {
        return String.format(
                "%s/labels/%s",
                getImageSubPath(pixels),
                pixels.getImage().getSeries(),
                uuid);
    }

    /**
     * Returns a label image NGFF pixel buffer if it exists.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return A pixel buffer instance.
     * @throws IOException
     */
    public ZarrPixelBuffer getLabelImagePixelBuffer(Pixels pixels, String uuid)
            throws IOException {
        if (ngffDir == null) {
            throw new IllegalArgumentException("NGFF dir not configured");
        }
        Path root = ngffDir.resolve(getLabelImageSubPath(pixels, uuid));
        return new ZarrPixelBuffer(root, maxTileLength);
    }

    /**
     * Returns a pixel buffer for a given set of pixels. Either an NGFF pixel
     * buffer, a proprietary ROMIO pixel buffer or a specific pixel buffer
     * implementation.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return A pixel buffer instance.
     */
    @Override
    public PixelBuffer getPixelBuffer(Pixels pixels, boolean write) {
        if (ngffDir != null) {
            try {
                Path root = ngffDir.resolve(getImageSubPath(pixels));
                return new ZarrPixelBuffer(root,  maxTileLength);
            } catch (Exception e) {
                log.info(
                    "Getting NGFF Pixel Buffer failed - " +
                    "attempting to get local data", e);
            }
        }
        return _getPixelBuffer(pixels, write);
    }

}

