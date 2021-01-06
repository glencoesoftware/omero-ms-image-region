package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.slf4j.LoggerFactory;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonObject;
import loci.formats.FormatTools;
import ome.util.PixelData;
import ucar.ma2.InvalidRangeException;

public class ZarrUtils {

    String accessKey;
    String secretKey;
    String awsRegion;
    String s3EndpointOverride;
    Integer maxTileLength;

    public ZarrUtils(String accessKey, String secretKey, String awsRegion, String s3EndpointOverride, Integer maxTileLength) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.awsRegion = awsRegion;
        this.s3EndpointOverride = s3EndpointOverride;
        this.maxTileLength = maxTileLength;
    }

    private static final org.slf4j.Logger log =
        LoggerFactory.getLogger(ZarrUtils.class);

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
            case f4:
                return FormatTools.getPixelTypeString(FormatTools.FLOAT);
            case f8:
                return FormatTools.getPixelTypeString(FormatTools.DOUBLE);
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    public static long[] getMinMax(ByteBuffer buf, DataType type) {
        if (!buf.hasRemaining()) {
            throw new IllegalArgumentException("Cannot get max of empty buffer");
        }
        switch(type) {
            case u1: {
                long max = buf.get() & 0xff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.get() & 0xff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case i1: {
                long max = buf.get();
                long min = max;
                while (buf.hasRemaining()) {
                    byte next = buf.get();
                    log.info(Byte.toString(next));
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case u2: {
                long max = buf.getShort() & 0xffff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getShort() & 0xffff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case i2: {
                long max = buf.getShort();
                long min = max;
                while (buf.hasRemaining()) {
                    short next = buf.getShort();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case u4:{
                long max = buf.getInt() & 0xffffffffl;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getInt() & 0xffffffffl;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case i4: {
                long max = buf.getInt();
                long min = max;
                while (buf.hasRemaining()) {
                    int next = buf.getInt();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            default:
                throw new IllegalArgumentException("Type: " + type.toString() + " not supported");
        }
    }

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
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    public static int[][] getShapeAndStartFromString(String domainStr) {
        //String like [0,1,0,100:150,200:250]
        if(domainStr.length() == 0) {
            return null;
        }
        if(domainStr.startsWith("[")) {
            domainStr = domainStr.substring(1);
        }
        if(domainStr.endsWith("]")) {
            domainStr = domainStr.substring(0, domainStr.length() - 1);
        }
        String[] dimStrs = domainStr.split(",");
        if(dimStrs.length != 5) {
            throw new IllegalArgumentException("Invalid number of dimensions in domain string");
        }
        int[][] shapeAndStart = new int[][] {new int[5], new int[5]};
        for(int i = 0; i < 5; i++) {
            String s = dimStrs[i];
            if(s.contains(":")) {
                String[] startEnd = s.split(":");
                shapeAndStart[0][i] = Integer.valueOf(startEnd[1]) - Integer.valueOf(startEnd[0]); //shape
                shapeAndStart[1][i] = Integer.valueOf(startEnd[0]); //start
            } else {
                shapeAndStart[0][i] = 1; //shape - size 1 in this dim
                shapeAndStart[1][i] = Integer.valueOf(s); //start
            }
        }
        return shapeAndStart;
    }

    public String getImageDataPath(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel) {
        if (ngffDir.startsWith("s3://")) {
            StringBuilder s3PathBuilder = new StringBuilder().append(ngffDir);
            if (!ngffDir.endsWith("/")) {
                s3PathBuilder.append("/");
            }
            s3PathBuilder.append(filesetId).append(".zarr/")
                .append(series).append("/")
                .append(resolutionLevel);
            return s3PathBuilder.toString();
        } else {
            Path zarrDataPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".zarr").resolve(Integer.toString(series)).resolve(Integer.toString(resolutionLevel));
            log.info(zarrDataPath.toString());
            return zarrDataPath.toString();
        }
    }

    private String getS3LabelImagePath(String ngffDir, long filesetId, int series, String uuid, Integer resolution) {
        StringBuilder s3PathBuilder = new StringBuilder();
        s3PathBuilder.append(ngffDir);
        if(!s3PathBuilder.toString().endsWith("/")) {
            s3PathBuilder.append("/");
        }
        s3PathBuilder.append(filesetId).append(".zarr").append("/")
            .append(series).append("/")
            .append("labels").append("/")
            .append(uuid).append("/")
            .append(resolution).append("/");
        return s3PathBuilder.toString();
    }

    private String getLocalLabelImagePath(String ngffDir, long filesetId, int series, String uuid, Integer resolution) {
        Path labelImageBasePath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                + ".zarr/" + Integer.toString(series));
        Path labelImageLabelsPath = labelImageBasePath.resolve("labels");
        Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
        Path fullNgffDir = labelImageShapePath.resolve(Integer.toString(resolution));
        return fullNgffDir.toString();
    }

    public byte[] getLabelImageBytes(String ngffDir, long filesetId, int series, String uuid, Integer resolution,
            String domainStr) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_label_image_bytes");
        try {
            String ngffPath;
            if (ngffDir.startsWith("s3://")) {
                span.tag("location", "s3");
                ngffPath = getS3LabelImagePath(ngffDir, filesetId, series, uuid, resolution);
            } else {
                span.tag("location", "local");
                ngffPath = getLocalLabelImagePath(ngffDir, filesetId, series, uuid, resolution);
            }
            return null;
        } finally {
            span.finish();
        }
    }

    public static String getS3ImageDataPath(String ngffDir, long filesetId, int series, Integer resolution) {
        StringBuilder s3PathBuilder = new StringBuilder();
        s3PathBuilder.append(ngffDir);
        if(!s3PathBuilder.toString().endsWith("/")) {
            s3PathBuilder.append("/");
        }
        s3PathBuilder.append(filesetId).append(".tiledb").append("/")
            .append(series).append("/")
            .append(resolution).append("/");
        return s3PathBuilder.toString();
    }

    public PixelData getPixelData(String ngffPath, String domainStr, Integer maxTileLength) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_pixel_data_from_path");
        try {
            return null;
        } finally {
            span.finish();
        }
    }

    public PixelData getPixelData(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel,
            String domainStr) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_pixel_data_from_dir");
        String ngffPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel);
        try {
            log.info("getPixelData " + ngffPath);
            ZarrArray array = ZarrArray.open(ngffPath);
            byte[] buffer = ZarrUtils.getData(array, domainStr, maxTileLength);
            PixelData d = new PixelData(getPixelsType(array.getDataType()), ByteBuffer.wrap(buffer));
            d.setOrder(ByteOrder.nativeOrder());
            return d;
        } catch (IOException | InvalidRangeException e) {
            log.error("Error getting Zarr pixel data",e);
            return null;
        } finally {
            span.finish();
        }
    }

    public static void rescaleSubarrayDomain(int[] requestedShape, int[] fromPosition, int[] shape) {
        if(fromPosition[0] >  shape[0] ||
           fromPosition[1] >  shape[1] ||
           fromPosition[2] >  shape[2] ||
           fromPosition[3] >  shape[3] ||
           fromPosition[4] >  shape[4]) {
            throw new IllegalArgumentException("Starting index exceeds image size");
        }
        /*
        requestedShape[0] = requestedShape[0] + fromPosition[0] > shape[0] ?
                (long) domain.getDimension("t").getDomain().getSecond() : subarrayDomain[1];
        subarrayDomain[3] = subarrayDomain[3] > (long) domain.getDimension("c").getDomain().getSecond() ?
                (long) domain.getDimension("c").getDomain().getSecond() : subarrayDomain[3];
        subarrayDomain[5] = subarrayDomain[5] > (long) domain.getDimension("z").getDomain().getSecond() ?
                (long) domain.getDimension("z").getDomain().getSecond() : subarrayDomain[5];
        subarrayDomain[7] = subarrayDomain[7] > (long) domain.getDimension("y").getDomain().getSecond() ?
                (long) domain.getDimension("y").getDomain().getSecond() : subarrayDomain[7];
        subarrayDomain[9] = subarrayDomain[9] > (long) domain.getDimension("x").getDomain().getSecond() ?
                (long) domain.getDimension("x").getDomain().getSecond() : subarrayDomain[9];
                */
    }

    public static byte[] getData(ZarrArray zarray) throws IOException, InvalidRangeException {
        int[] shape = zarray.getShape();
        int num_dims = shape.length;
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }
        return getBytes(zarray, zarray.getShape(), new int[] {0,0,0,0,0});
    }

    public static byte[] getData(ZarrArray zarray, String subarrayString, Integer maxTileLength) throws IOException, InvalidRangeException {
        int[] shape = zarray.getShape();
        int num_dims = shape.length;
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }
        log.info(subarrayString);
        int[][] shapeAndStart = getShapeAndStartFromString(subarrayString);
        log.info(Arrays.toString(shapeAndStart[0]));
        log.info(Arrays.toString(shapeAndStart[1]));
        if(shapeAndStart[0][3] > maxTileLength || shapeAndStart[0][4] > maxTileLength) {
            throw new IllegalArgumentException("Tile size exceeds max size of " + Integer.toString(maxTileLength));
        }
        return ZarrUtils.getBytes(zarray, shapeAndStart[0], shapeAndStart[1]);
    }

    public static byte[] getBytes(ZarrArray zarray, int[] shape, int[] offset) {
        DataType type = zarray.getDataType();
        try {
            switch(type) {
                case u1:
                case i1:
                    return (byte[]) zarray.read(shape, offset);
                case u2:
                case i2:
                {
                    short[] data = (short[]) zarray.read(shape, offset);
                    ByteBuffer bbuf = ByteBuffer.allocate(data.length * 2);
                    ShortBuffer sbuf = bbuf.asShortBuffer();
                    sbuf.put(data);
                    return bbuf.array();
                }
                case u4:
                case i4:
                {
                    int[] data = (int[]) zarray.read(shape, offset);
                    ByteBuffer bbuf = ByteBuffer.allocate(data.length * 4);
                    IntBuffer ibuf = bbuf.asIntBuffer();
                    ibuf.put(data);
                    return bbuf.array();
                }
                case i8:
                {
                    long[] data = (long[]) zarray.read(shape, offset);
                    ByteBuffer bbuf = ByteBuffer.allocate(data.length * 8);
                    LongBuffer lbuf = bbuf.asLongBuffer();
                    lbuf.put(data);
                    return bbuf.array();
                }
                case f4:
                case f8:
                default:
                    log.error("Unsupported data type" + type.toString());
                    return null;
            }
        } catch(InvalidRangeException|IOException e) {
            log.error("Error getting zarr PixelData", e);
            return null;
        }
    }

    public int getResolutionLevels(String ngffDir, Long filesetId, Integer series) {
        if (ngffDir.startsWith("s3://")) {
            return 0;
        } else {
            Path zarrSeriesPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".tiledb").resolve(Integer.toString(series));
            File[] directories = new File(zarrSeriesPath.toString()).listFiles(File::isDirectory);
            int count = 0;
            for(File dir : directories) {
                try {
                    Integer.valueOf(dir.getName());
                    count++;
                } catch(NumberFormatException e) {
                }
            }
            return count;
        }
    }
/*
    public static String getStringMetadata(Array array, String key) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_string_metadata");
        try {
            if(array.hasMetadataKey(key)) {
                NativeArray strNativeArray = array.getMetadata(key, Datatype.TILEDB_CHAR);
                return new String((byte[]) strNativeArray.toJavaArray(), StandardCharsets.UTF_8);
            } else {
                return null;
            }
        } finally {
            span.finish();
        }
    }
    */

    public static long[] getMinMaxMetadata(ZarrArray array) {
        /*
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_minmax_metadata");
        try {
            String key = "minmax";
            if(array.hasMetadataKey(key)) {
                NativeArray minMaxNativeArray = array.getMetadata(key, Datatype.TILEDB_INT32);
                return (long[]) minMaxNativeArray.toJavaArray();
            } else {
                return null;
            }
        } finally {
            span.finish();
        }
        */
        return null;
    }

    public static int getResolutionLevelCount(String labelImageShapePath) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_res_lvl_count_local");
        File[] directories = new File(labelImageShapePath).listFiles(File::isDirectory);
        int count = 0;
        for(File dir : directories) {
            try {
                Integer.valueOf(dir.getName());
                count++;
            } catch(NumberFormatException e) {
            }
        }
        span.finish();
        return count;
    }

    public int getResolutionLevelCountS3(String parentPath) {
        return 0;
    }

    public int getDimSize(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel, Integer dimIdx) {
        try {
            String imageDataPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel);
            log.info(imageDataPath);
            ZarrArray zarray = ZarrArray.open(imageDataPath);
            return zarray.getShape()[dimIdx];
        } catch (IOException e) {
            log.error("Error while getting zarr dimension size", e);
        }
        return -1;
    }

    public Integer[] getSizeXandY(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel) {
        Integer[] xy = new Integer[2];
        try {
            ZarrArray zarray = ZarrArray.open(getImageDataPath(ngffDir, filesetId, series, resolutionLevel));
            xy[0] = zarray.getShape()[4];
            xy[1] = zarray.getShape()[3];
            return xy;
        } catch (IOException e) {
            log.error("Error in zarr getSizeXandY", e);
            return null;
        }
    }


    /**
     * Get label image metadata request handler.
     * @param ngffDir The base directory for ngff data
     * @param filesetId The fileset ID of the image
     * @param series The series ID of the image in the fileset
     * @param uuid The External Info UUID of the shape associated with the label image
     * @param ngffDir the base directory for ngff data
     * @return A JsonObject with the label image metadata
     */
    public JsonObject getLabelImageMetadata(String ngffDir, long filesetId, int series, String uuid, int resolution) {
        return getLabelImageMetadataLocal(ngffDir, filesetId, series, uuid, resolution);
    }

    /**
     * Get label image metadata for local label image.
     * @param ngffDir The base directory for ngff data
     * @param filesetId The fileset ID of the image
     * @param series The series ID of the image in the fileset
     * @param uuid The External Info UUID of the shape associated with the label image
     * @param ngffDir the base directory for ngff data
     * @return A JsonObject with the label image metadata
     */
    private JsonObject getLabelImageMetadataLocal(String ngffDir, long filesetId, int series, String uuid, int resolution) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_dim_size");
        span.finish();
        return null;
    }

}
