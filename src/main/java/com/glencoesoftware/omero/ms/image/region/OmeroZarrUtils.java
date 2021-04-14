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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.bc.zarr.ZarrUtils;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import loci.common.DataTools;
import loci.formats.FormatTools;
import ome.model.stats.StatsInfo;
import ome.util.PixelData;
import ucar.ma2.InvalidRangeException;

public class OmeroZarrUtils {

    private static final String MULTISCALES_KEY = "multiscales";
    private static final String MINMAX_KEY = "minmax";
    private static final String OMERO_KEY = "omero";
    private static final String LABELS = "labels";

    public static final String ZARR_EXTN = ".zarr";

    /** AWS/Cloud Access key */
    String accessKey;

    /** AWS/Cloud secret key */
    String secretKey;

    /** AWS/Cloud Region */
    String awsRegion;

    /** Cloud Endpoint Override */
    String s3EndpointOverride;

    /** Max Tile Length */
    Integer maxTileLength;

    /** S3 filesystem object */
    FileSystem s3fs;

    /**
     * Default constructor
     * @param accessKey AWS/Cloud Access Key
     * @param secretKey AWS/Cloud Secret Key
     * @param awsRegion AWS/Cloud Region
     * @param s3EndpointOverride For non-aws object storage endpoint
     * @param maxTileLength Max tile length
     * @param s3fsWrapper Configured S3 filesystem wrapper
     */
    public OmeroZarrUtils(String accessKey,
            String secretKey,
            String awsRegion,
            String s3EndpointOverride,
            Integer maxTileLength,
            S3FilesystemWrapper s3fsWrapper) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.awsRegion = awsRegion;
        this.s3EndpointOverride = s3EndpointOverride;
        this.maxTileLength = maxTileLength;
        this.s3fs = s3fsWrapper.getS3fs();
    }

    private static final org.slf4j.Logger log =
        LoggerFactory.getLogger(OmeroZarrUtils.class);

    /**
     * Get PixelType String from Zarr DataType
     * @param type The Zarr type
     * @return
     */
    public static String getPixelsType(DataType type) {
        switch (type) {
            case u1:
                return FormatTools.getPixelTypeString(FormatTools.UINT8);
            case i1:
                return FormatTools.getPixelTypeString(FormatTools.INT8);
            case u2:
                return FormatTools.getPixelTypeString(FormatTools.UINT16);
            case i2:
                return FormatTools.getPixelTypeString(FormatTools.INT16);
            case u4:
                return FormatTools.getPixelTypeString(FormatTools.UINT32);
            case i4:
                return FormatTools.getPixelTypeString(FormatTools.INT32);
            case i8:
                return "int64";
            case f4:
                return FormatTools.getPixelTypeString(FormatTools.FLOAT);
            case f8:
                return FormatTools.getPixelTypeString(FormatTools.DOUBLE);
            default:
                throw new IllegalArgumentException(
                        "Attribute type " + type.toString() + " not supported");
        }
    }

    /**
     * Get bytes per pixel for a given DataType
     * @param type Zarr DataType
     * @return Number of bytes per pixel for the given DataType
     */
    public static int getBytesPerPixel(DataType type) {
        switch (type) {
            case u1:
            case i1:
                return 1;
            case u2:
            case i2:
                return 2;
            case u4:
            case i4:
                return 4;
            case i8:
                return 8;
            default:
                throw new IllegalArgumentException(
                        "Attribute type " + type.toString() + " not supported");
        }
    }

    /**
     * Get the region shape and the start (offset) from the string
     * @param domainStr The string which describes the domain
     * @return 2D int array [[shape_dim1,...],[start_dim1,...]]
     */
    public static int[][] getShapeAndStartFromString(String domainStr) {
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

    /**
     * Get the image data path from the constituent components
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId FilesetID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @return Path to the image pixel data
     * @throws IOException
     */
    public Path getImageDataPath(
        String ngffDir, Long filesetId, Integer series,
        Integer resolutionLevel)
            throws IOException {
        Path imageDataPath = getLocalOrS3Path(ngffDir);
        imageDataPath = imageDataPath
                .resolve(Long.toString(filesetId) + ZARR_EXTN)
                .resolve(Integer.toString(series))
                .resolve(Integer.toString(resolutionLevel));
        return imageDataPath;
    }

    /**
     * Get the path to the label image data from constituent components
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param uuid Shape UUID
     * @param resolution Requested resolution level
     * @return Path to the reqeusted label image data
     * @throws IOException
     */
    private Path getLabelImagePath(
        String ngffDir, long filesetId, int series, String uuid,
        Integer resolution)
            throws IOException {
        Path labelImageBasePath = getLocalOrS3Path(ngffDir);
        labelImageBasePath = labelImageBasePath.resolve(Long.toString(filesetId)
                + ZARR_EXTN).resolve(Integer.toString(series));
        Path labelImageLabelsPath = labelImageBasePath.resolve(LABELS);
        Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
        Path fullNgffDir = labelImageShapePath
                .resolve(Integer.toString(resolution));
        return fullNgffDir;
    }

    /**
     * Get byte array of label image data
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param uuid Shape UUID
     * @param resolution Requested resolution
     * @param domainStr String like [0,1,0,100:150,200:250] denoting the region
     * to return
     * @return The NGFF label image data
     */
    public byte[] getLabelImageBytes(
            String ngffDir, long filesetId, int series, String uuid,
            Integer resolution, String domainStr) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_label_image_bytes_zarr");
        try {
            Path ngffPath = getLabelImagePath(
                    ngffDir, filesetId, series, uuid, resolution);
            ZarrArray zarray = ZarrArray.open(ngffPath);
            return OmeroZarrUtils.getData(zarray, domainStr, maxTileLength);
        } catch (IOException | InvalidRangeException e) {
            log.error("Failed to get label image bytes", e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get the NGFF image pixel data
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @param domainStr String like [0,1,0,100:150,200:250] denoting the region
     * to return
     * @return
     * @throws IOException
     */
    public PixelData getPixelData(
        String ngffDir, Long filesetId, Integer series,
        Integer resolutionLevel, String domainStr)
            throws IOException {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixel_data_from_zarr");
        try {
            Path ngffPath = getImageDataPath(
                ngffDir, filesetId, series, resolutionLevel);
            ZarrArray array = ZarrArray.open(ngffPath);
            byte[] buffer = OmeroZarrUtils.getData(
                    array, domainStr, maxTileLength);
            PixelData d = new PixelData(
                getPixelsType(array.getDataType()), ByteBuffer.wrap(buffer));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (IOException | InvalidRangeException e) {
            log.error("Error getting Zarr pixel data",e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get image pixel data for entire image
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @return The pixel data for the image
     * @throws IOException
     */
    public PixelData getPixelData(
        String ngffDir, Long filesetId, Integer series, Integer resolutionLevel)
            throws IOException {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixel_data_from_zarr");
        try {
            Path ngffPath = getImageDataPath(
                ngffDir, filesetId, series, resolutionLevel);
            ZarrArray array = ZarrArray.open(ngffPath);
            byte[] buffer = OmeroZarrUtils.getData(array);
            PixelData d = new PixelData(
                getPixelsType(array.getDataType()), ByteBuffer.wrap(buffer));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (IOException | InvalidRangeException e) {
            log.error("Error getting Zarr pixel data",e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get data from ZarrArray as byte[]
     * @param zarray The ZarrArray to get data from
     * @return A byte array of data in the ZarrArray
     * @throws IOException
     * @throws InvalidRangeException
     */
    public static byte[] getData(ZarrArray zarray)
            throws IOException, InvalidRangeException {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_entire_data_from_zarr");
        try {
            int[] shape = zarray.getShape();
            int num_dims = shape.length;
            if (num_dims != 5) {
                throw new IllegalArgumentException(
                    "Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
            }
            return OmeroZarrUtils.getBytes(
                    zarray, zarray.getShape(), new int[] {0,0,0,0,0});
        } finally {
            span.finish();
        }
    }

    /**
     * Get data from subarray of the ZarrArray
     * @param zarray The ZarrArray to get data from
     * @param subarrayString string like [0,1,0,100:150,200:250] describing the region to retrieve
     * @param maxTileLength The max region size to return
     * @return
     * @throws IOException
     * @throws InvalidRangeException
     */
    public static byte[] getData(
            ZarrArray zarray, String subarrayString, Integer maxTileLength)
                    throws IOException, InvalidRangeException {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_subregion_data_from_zarr");
        try {
            int[] shape = zarray.getShape();
            int num_dims = shape.length;
            if (num_dims != 5) {
                throw new IllegalArgumentException(
                    "Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
            }
            log.debug(subarrayString);
            int[][] shapeAndStart = getShapeAndStartFromString(subarrayString);
            if (shapeAndStart[0][3] > maxTileLength
                || shapeAndStart[0][4] > maxTileLength) {
                throw new IllegalArgumentException(
                    "Tile size exceeds max size of " +
                    Integer.toString(maxTileLength));
            }
            return OmeroZarrUtils.getBytes(
                    zarray, shapeAndStart[0], shapeAndStart[1]);
        } finally {
            span.finish();
        }
    }

    /**
     * Get byte array from ZarrArray
     * @param zarray The ZarrArray to get data from
     * @param shape The shape of the region to retrieve
     * @param offset The offset of the region
     * @return byte array of data from the ZarrArray
     */
    public static byte[] getBytes(ZarrArray zarray, int[] shape, int[] offset) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_bytes_zarr");
        try {
            ByteOrder byteOrder = zarray.getByteOrder();
            DataType type = zarray.getDataType();
            switch (type) {
                case u1:
                case i1:
                    return (byte[]) zarray.read(shape, offset);
                case u2:
                case i2:
                {
                    ScopedSpan readSpan = Tracing.currentTracer()
                            .startScopedSpan("zarr_read");
                    try {
                        short[] data = (short[]) zarray.read(shape, offset);
                        ByteBuffer bbuf = ByteBuffer.allocate(data.length * 2);
                        ShortBuffer sbuf = bbuf.asShortBuffer();
                        if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN)) {
                            for (int i = 0; i < data.length; i++) {
                                sbuf.put(i, DataTools.swap(data[i]));
                            }
                        } else {
                            sbuf.put(data);
                        }
                        return bbuf.array();
                    } finally {
                        readSpan.finish();
                    }
                }
                case u4:
                case i4:
                {
                    ScopedSpan readSpan = Tracing.currentTracer()
                            .startScopedSpan("zarr_read");
                    try {
                        int[] data = (int[]) zarray.read(shape, offset);
                        ByteBuffer bbuf = ByteBuffer.allocate(data.length * 4);
                        IntBuffer ibuf = bbuf.asIntBuffer();
                        if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN)) {
                            for (int i = 0; i < data.length; i++) {
                                ibuf.put(i, DataTools.swap(data[i]));
                            }
                        } else {
                            ibuf.put(data);
                        }
                        return bbuf.array();
                    } finally {
                        readSpan.finish();
                    }
                }
                case i8:
                {
                    ScopedSpan readSpan = Tracing.currentTracer()
                            .startScopedSpan("zarr_read");
                    try {
                        long[] data = (long[]) zarray.read(shape, offset);
                        ByteBuffer bbuf = ByteBuffer.allocate(data.length * 8);
                        LongBuffer lbuf = bbuf.asLongBuffer();
                        if (byteOrder.equals(ByteOrder.LITTLE_ENDIAN)) {
                            for (int i = 0; i < data.length; i++) {
                                lbuf.put(i, DataTools.swap(data[i]));
                            }
                        } else {
                            lbuf.put(data);
                        }
                        return bbuf.array();
                    } finally {
                        readSpan.finish();
                    }
                }
                case f4:
                case f8:
                default:
                    log.error("Unsupported data type" + type.toString());
                    return null;
            }
        } catch (InvalidRangeException|IOException e) {
            log.error("Error getting zarr PixelData", e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get the number of resolution levels in NGFF image data
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return Number of resolution levels
     */
    public int getResolutionLevels(
            String ngffDir, Long filesetId, Integer series) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_resolution_levels_zarr");
        try {
            Path basePath;
            try {
                basePath = getLocalOrS3Path(ngffDir);
            } catch (IOException e) {
                log.error("Failed to get resolution levels from S3", e);
                return 0;
            }
            Path zarrSeriesPath = basePath
                    .resolve(Long.toString(filesetId) + ZARR_EXTN)
                    .resolve(Integer.toString(series));
            int count = 0;
            try {
                DirectoryStream<Path> stream =
                        Files.newDirectoryStream(zarrSeriesPath);
                for (Path entry : stream) {
                    try {
                        Integer.parseInt(entry.getFileName().toString());
                        count++;
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }
            } catch (IOException e) {
                log.error("Error counting resolution levels", e);
                span.error(e);
                return -1;
            }
            return count;
        } finally {
            span.finish();
        }
    }

    /**
     * Get the correct path (either local or cloud) to the NGFF files
     * @param ngffDir Top-level directory containing NGFF files
     * @return Path object to the NGFF directory
     * @throws IOException
     */
    public Path getLocalOrS3Path(String ngffDir) throws IOException {
        Path path;
        if (ngffDir.startsWith("s3://")) {
            if (s3fs == null) {
                throw new IOException(
                        "Cannot get s3 path from null FileSystem");
            }
            String s3BucketName = ngffDir.substring(5); // Remove s3://
            Path bucketPath = s3fs.getPath("/" + s3BucketName);
            return bucketPath;
        } else {
            path = Paths.get(ngffDir);
        }
        return path;
    }

    /**
     * Get the size of the image in the given dimension
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @param dimIdx The index of the dimension
     * @return Size of the requested dimension
     */
    public int getDimSize(
            String ngffDir, Long filesetId, Integer series,
            Integer resolutionLevel, Integer dimIdx) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_dim_size_zarr");
        try {
            Path imageDataPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel);
            ZarrArray zarray = ZarrArray.open(imageDataPath);
            return zarray.getShape()[dimIdx];
        } catch (IOException e) {
            log.error("Error while getting zarr dimension size", e);
        } finally {
            span.finish();
        }
        return -1;
    }

    /**
     * Get the X and Y sizes of the given image at the given resolution level
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param resolutionLevel Requested resolution level
     * @return Integer array [sizeX, sizeY]
     */
    public Integer[] getSizeXandY(
            String ngffDir, Long filesetId, Integer series,
            Integer resolutionLevel) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_size_xy_zarr");
        try {
            Integer[] xy = new Integer[2];
            ZarrArray zarray = ZarrArray.open(getImageDataPath(
                    ngffDir, filesetId, series, resolutionLevel));
            xy[0] = zarray.getShape()[4];
            xy[1] = zarray.getShape()[3];
            return xy;
        } catch (IOException e) {
            log.error("Error in zarr getSizeXandY", e);
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Get produce metadata JsonObject from ZarrArray info and other metadata info
     * @param zarray ZarrArray to get metadata from
     * @param minMax Min and max values of the data
     * @param multiscales Multiscales metadata object
     * @param uuid Shape uuid
     * @return Metadata JsonObject
     */
    private static JsonObject getMetadataFromArray(
            ZarrArray zarray, int[] minMax, JsonObject multiscales,
            String uuid) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_metadata_from_array");
        try {
            int[] shape = zarray.getShape();

            JsonObject metadata = new JsonObject();
            if(minMax != null) {
                metadata.put("min", minMax[0]);
                metadata.put("max", minMax[1]);
            }
            JsonObject size = new JsonObject();
            size.put("t", shape[0]);
            size.put("c", shape[1]);
            size.put("z", shape[2]);
            size.put("height", shape[3]);
            size.put("width", shape[4]);

            metadata.put("size", size);
            metadata.put("type", getPixelsType(zarray.getDataType()));
            if(multiscales != null) {
                metadata.put(MULTISCALES_KEY, multiscales);
            }
            metadata.put("uuid", uuid);
            return metadata;
        } finally {
            span.finish();
        }
    }

    /**
     * Get label image metadata request handler.
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId The fileset ID of the image
     * @param series The series ID of the image in the fileset
     * @param uuid The External Info UUID of the shape associated with the label image
     * @param ngffDir the base directory for ngff data
     * @return A JsonObject with the label image metadata
     */
    public JsonObject getLabelImageMetadata(
            String ngffDir, long filesetId, int series, String uuid,
            int resolution) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("zarr_get_label_image_metadata");
        try {
            Path basePath;
            try {
                basePath = getLocalOrS3Path(ngffDir);
            } catch (IOException e) {
                log.error("Error getting metadata from s3", e);
                span.error(e);
                return null;
            }
            Path labelImageBasePath = basePath.resolve(Long.toString(filesetId)
                    + ".zarr").resolve(Integer.toString(series));
            Path labelImageLabelsPath = labelImageBasePath.resolve(LABELS);
            Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
            Path fullngffDir = labelImageShapePath
                    .resolve(Integer.toString(resolution));
            JsonObject multiscales = null;
            int[] minMax = null;
            if (Files.exists(fullngffDir)) {
                try {
                    ZarrGroup labelImageShapeGroup =
                            ZarrGroup.open(labelImageShapePath);
                    JsonObject jsonAttrs = new JsonObject(
                            ZarrUtils.toJson(labelImageShapeGroup.getAttributes()));
                    if (jsonAttrs.containsKey(MULTISCALES_KEY)) {
                        try {
                        multiscales = jsonAttrs.getJsonArray(MULTISCALES_KEY)
                                .getJsonObject(0);
                        } catch (Exception e) {
                            try {
                                log.warn(
                                    "Failed to get multiscales as array - " +
                                    "attempting as object");
                                multiscales = jsonAttrs.getJsonObject(
                                        MULTISCALES_KEY);
                            } catch (Exception e2) {
                                log.error(
                                    "Failed to get multiscales metadata as array " +
                                    "or object");
                                span.error(e);
                            }
                        }
                        JsonArray datasets = multiscales.getJsonArray("datasets");
                        JsonArray resLvlArray = datasets;
                        for (int i = 0; i < resLvlArray.size(); i++) {
                            Path resPath = labelImageShapePath
                                    .resolve(Integer.toString(i));
                            ZarrArray za = ZarrArray.open(resPath);
                            JsonArray chunkArray = new JsonArray();
                            int[] chunks = za.getChunks();
                            for (int j = 0; j < chunks.length; j++) {
                                chunkArray.add(chunks[j]);
                            }
                            datasets.getJsonObject(i).put("chunksize", chunkArray);
                        }
                    } if (jsonAttrs.containsKey(MINMAX_KEY)) {
                        JsonArray minMaxArray = jsonAttrs.getJsonArray(MINMAX_KEY);
                        minMax = new int[] {
                            minMaxArray.getInteger(0), minMaxArray.getInteger(1)
                        };
                    }
                } catch (Exception e) {
                    log.error(
                        "Exception while retrieving zarr label image metadata", e);
                    span.error(e);
                }
                try {
                    ZarrArray zarray = ZarrArray.open(fullngffDir);
                    return getMetadataFromArray(zarray, minMax, multiscales, uuid);
                } catch (Exception e) {
                    log.error("Exception while retrieving label image metadata", e);
                    span.error(e);
                }
            }
        } finally {
            span.finish();
        }
        return null;
    }

    /**
     * Get a list of resolution descriptions (X and Y sizes)
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return List of Resolution level sizes
     */
    public List<List<Integer>> getResolutionDescriptions(
            String ngffDir, long filesetId, int series) {
        List<List<Integer>> resolutionDescriptions =
                new ArrayList<List<Integer>>();
        int resLvlCount = getResolutionLevels(ngffDir, filesetId, series);
        for(int i = 0; i < resLvlCount; i++) {
            List<Integer> description = new ArrayList<Integer>();
            Integer[] xy = getSizeXandY(ngffDir, filesetId, series, i);
            description.add(xy[0]);
            description.add(xy[1]);
            resolutionDescriptions.add(description);
        }
        return resolutionDescriptions;
    }

    /**
     * Get 'omero' metadata for the image
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return JsonObject containing 'omero' metadata
     */
    public JsonObject getOmeroMetadata(String ngffDir, long filesetId, int series) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("zarr_get_omero_metadata");
        try {
            Path basePath;
            try {
                basePath = getLocalOrS3Path(ngffDir);
            } catch (IOException e) {
                log.error("Error getting metadata from s3", e);
                span.error(e);
                return null;
            }
            Path ngffPath = basePath.resolve(Long.toString(filesetId)
                    + ".zarr").resolve(Integer.toString(series));
            try {
                ZarrGroup zarrGroup = ZarrGroup.open(ngffPath);
                JsonObject jsonAttrs = new JsonObject(
                        ZarrUtils.toJson(zarrGroup.getAttributes()));
                if (!jsonAttrs.containsKey(OMERO_KEY)) {
                    return null;
                }
                try {
                    return jsonAttrs.getJsonObject(OMERO_KEY);
                } catch (Exception e) {
                    log.debug("Getting omero metadata as string");
                    return new JsonObject(jsonAttrs.getString(OMERO_KEY));
                }
            } catch (Exception e) {
                log.error("Error getting omero metadata from zarr");
                span.error(e);
                return null;
            }
        } finally {
            span.finish();
        }
    }

}
