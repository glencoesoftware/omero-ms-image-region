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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import com.bc.zarr.ZarrGroup;
import com.upplication.s3fs.S3FileSystemProvider;

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

    /** Max Tile Length */
    private final Integer maxTileLength;

    public PixelsService(
            String path, long memoizerWait, FilePathResolver resolver,
            BackOff backOff, TileSizes sizes, IQuery iQuery,
            Integer maxTileLength) throws IOException {
        super(
            path, true, new File(new File(path), "BioFormatsCache"),
            memoizerWait, resolver, backOff, sizes, iQuery);
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
                FileSystem fs = null;
                try {
                    fs = FileSystems.getFileSystem(endpoint);
                } catch (FileSystemNotFoundException e) {
                    Map<String, String> env = new HashMap<String, String>();
                    env.put(
                            S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS,
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

    private Path getFilesetPath(Pixels pixels) throws IOException {
        Properties properties = new Properties();
        Path originalFilePath = Paths.get(
                resolver.getOriginalFilePath(this, pixels));
        properties.load(Files.newInputStream(
                originalFilePath.getParent().resolve("ome_ngff.properties"),
                StandardOpenOption.READ
        ));
        return asPath(properties.getProperty("uri"));
    }

    private String getImageSubPath(Path root, Pixels pixels)
            throws IOException {
        Image image = pixels.getImage();
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
            int rowIndex = well.getRow();
            int columnIndex = well.getColumn();
            ZarrGroup z = ZarrGroup.open(root);
            Map<String, Object> attributes = z.getAttributes();
            Map<String, Object> plateAttributes =
                    (Map<String, Object>) attributes.get("plate");
            List<Map<String, Object>> wellsAttributes =
                    (List<Map<String, Object>>) plateAttributes.get("wells");
            String prefix = null;
            for (Map<String, Object> wellAttributes : wellsAttributes) {
                if (((Integer) wellAttributes.get("rowIndex")) == rowIndex
                        && ((Integer) wellAttributes.get("columnIndex"))
                                == columnIndex) {
                    prefix = (String) wellAttributes.get("path");
                }
            }
            if (prefix == null) {
                throw new IOException(
                        "Unable to locate path for Pixels:" + pixels.getId());
            }
            return String.format("%s/%d", prefix, field);
        }
        return String.format("%d", getSeries(pixels));
    }

    private String getLabelImageSubPath(Path root, Pixels pixels, String uuid)
            throws IOException {
        return String.format(
                "%s/labels/%s", getImageSubPath(root, pixels), uuid);
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
        Path root = getFilesetPath(pixels);
        root = root.resolve(getLabelImageSubPath(root, pixels, uuid));
        return new ZarrPixelBuffer(pixels, root, maxTileLength);
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
        try {
            Path root = getFilesetPath(pixels);
            root = root.resolve(getImageSubPath(root, pixels));
            log.info("OME-NGFF root is: " + root);
            try {
                PixelBuffer v =
                        new ZarrPixelBuffer(pixels, root, maxTileLength);
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
        return _getPixelBuffer(pixels, write);
    }

}

