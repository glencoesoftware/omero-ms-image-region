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
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.upplication.s3fs.S3FileSystemProvider;

import ome.api.IQuery;
import ome.io.nio.BackOff;
import ome.io.nio.FilePathResolver;
import ome.io.nio.PixelBuffer;
import ome.io.nio.TileSizes;
import ome.model.IObject;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.meta.ExternalInfo;
import ome.model.roi.Mask;
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
            LoggerFactory.getLogger(PixelsService.class);

    /** Max Plane Width */
    private final Integer maxPlaneWidth;

    /** Max Plane Height */
    private final Integer maxPlaneHeight;

    /** Whether or not OME NGFF is enabled */
    private final boolean isOmeNgffEnabled;

    public PixelsService(
            String path, long memoizerWait, FilePathResolver resolver,
            BackOff backOff, TileSizes sizes, IQuery iQuery,
            boolean isOmeNgffEnabled,
            int maxPlaneWidth, int maxPlaneHeight) throws IOException {
        super(
            path, true, new File(new File(path), "BioFormatsCache"),
            memoizerWait, resolver, backOff, sizes, iQuery);
        this.isOmeNgffEnabled = isOmeNgffEnabled;
        log.info("Is OME NGFF enabled? {}", isOmeNgffEnabled);
        this.maxPlaneWidth = maxPlaneWidth;
        this.maxPlaneHeight = maxPlaneHeight;
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
            Map<String, String> params = Splitter.on('&')
                    .trimResults()
                    .withKeyValueSeparator('=')
                    .split(uri.getQuery());
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
                //   * https://github.com/lasersonlab/Amazon-S3-FileSystem-NIO2
                FileSystem fs = null;
                try {
                    fs = FileSystems.getFileSystem(endpoint);
                } catch (FileSystemNotFoundException e) {
                    Map<String, String> env = new HashMap<String, String>();
                    String profile = params.get("profile");
                    if (profile != null) {
                        env.put("s3fs_credential_profile_name", profile);
                    }
                    String anonymous =
                            Optional.ofNullable(params.get("anonymous"))
                                    .orElse("false");
                    env.put("s3fs_anonymous", anonymous);
                    env.put(S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS,
                            OmeroAmazonS3ClientFactory.class.getName());
                    fs = FileSystems.newFileSystem(endpoint, env);
                }
                return fs.getPath(bucket, rest);
            }
        } catch (URISyntaxException e) {
            // Fall through
        }
        return Paths.get(ngffDir);
    }

    /**
     * Retrieves the row, column, and field for a given set of pixels.
     * @param pixels Set of pixels to return the row, column, and field for.
     * @return The row, column, and field as specified by the pixels parameters
     * or <code>null</code> if not in a plate.
     */
    protected int[] getRowColumnField(Pixels pixels)
    {
        Image image = pixels.getImage();
        int wellSampleCount = image.sizeOfWellSamples();
        if (wellSampleCount < 1) {
            return null;
        }
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
        return new int[] {well.getRow(), well.getColumn(), field};
    }

    @Override
    protected int getSeries(Pixels pixels) {
        return pixels.getImage().getSeries();
    }

    /**
     * Retrieve {@link Mask} or {@link Image} URI.
     * @param object loaded {@link Mask} or {@link Image} to check for a URI
     * @return URI or <code>null</code> if the object does not contain a URI
     * in its {@link ExternalInfo}.
     */
    private String getUri(IObject object) {
        ExternalInfo externalInfo = object.getDetails().getExternalInfo();
        if (externalInfo == null) {
            log.debug("{}:{} missing ExternalInfo",
                    object.getClass().getName(), object.getId());
            return null;
        }
        String uri = externalInfo.getLsid();
        if (uri == null) {
            log.debug("{}:{} missing LSID",
                    object.getClass().getName(), object.getId());
            return null;
        }
        return uri;
    }

    /**
     * Returns a label image NGFF pixel buffer if it exists.
     * @param mask Mask to retrieve a pixel buffer for.
     * @return A pixel buffer instance.
     * @throws IOException
     */
    public ZarrPixelBuffer getLabelImagePixelBuffer(Mask mask)
            throws IOException {
        Pixels pixels = new ome.model.core.Pixels();
        pixels.setSizeX(mask.getWidth().intValue());
        pixels.setSizeY(mask.getHeight().intValue());
        pixels.setSizeC(1);
        pixels.setSizeT(1);
        pixels.setSizeZ(1);
        String root = getUri(mask);
        if (root == null) {
            throw new IllegalArgumentException(
                    "No root for Mask:" + mask.getId());
        }
        return new ZarrPixelBuffer(
                pixels, asPath(root), maxPlaneWidth, maxPlaneHeight);
    }

    /**
     * Returns an NGFF pixel buffer for a given set of pixels.
     * @param pixels Pixels set to retrieve a pixel buffer for.
     * @param write Whether or not to open the pixel buffer as read-write.
     * <code>true</code> opens as read-write, <code>false</code> opens as
     * read-only.
     * @return An NGFF pixel buffer instance or <code>null</code> if one cannot
     * be found.
     */
    private PixelBuffer getOmeNgffPixelBuffer(Pixels pixels, boolean write) {
        try {
            String uri = getUri(pixels.getImage());
            if (uri == null) {
                log.debug("No OME-NGFF root");
                return null;
            }
            Path root = asPath(uri);
            log.info("OME-NGFF root is: " + root);
            try {
                PixelBuffer v = new ZarrPixelBuffer(
                        pixels, root, maxPlaneWidth, maxPlaneHeight);
                log.info("Using OME-NGFF pixel buffer");
                return v;
            } catch (Exception e) {
                log.warn(
                    "Getting OME-NGFF pixel buffer failed - " +
                    "attempting to get local data", e);
            }
        } catch (IOException e1) {
            log.debug(
                "Failed to find OME-NGFF metadata for Pixels:{}",
                pixels.getId());
        }
        return null;
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
        if (isOmeNgffEnabled) {
            PixelBuffer pixelBuffer = getOmeNgffPixelBuffer(pixels, write);
            if (pixelBuffer != null) {
                return pixelBuffer;
            }
        }
        return _getPixelBuffer(pixels, write);
    }

}

