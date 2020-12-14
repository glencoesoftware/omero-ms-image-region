package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Config;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.VFS;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import loci.formats.FormatTools;
import ome.util.PixelData;

public class TiledbUtils {

    String accessKey;
    String secretKey;
    String awsRegion;
    String s3EndpointOverride;
    Integer maxTileLength;

    public TiledbUtils(String accessKey, String secretKey, String awsRegion, String s3EndpointOverride, Integer maxTileLength) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.awsRegion = awsRegion;
        this.s3EndpointOverride = s3EndpointOverride;
        this.maxTileLength = maxTileLength;
    }

    private static final org.slf4j.Logger log =
        LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

    public static String getPixelsType(Datatype type) {
        switch (type) {
            case TILEDB_UINT8:
                return FormatTools.getPixelTypeString(FormatTools.UINT8);
            case TILEDB_INT8:
                return FormatTools.getPixelTypeString(FormatTools.INT8);
            case TILEDB_UINT16:
                return FormatTools.getPixelTypeString(FormatTools.UINT16);
            case TILEDB_INT16:
                return FormatTools.getPixelTypeString(FormatTools.INT16);
            case TILEDB_UINT32:
                return FormatTools.getPixelTypeString(FormatTools.UINT32);
            case TILEDB_INT32:
                return FormatTools.getPixelTypeString(FormatTools.INT32);
            case TILEDB_FLOAT32:
                return FormatTools.getPixelTypeString(FormatTools.FLOAT);
            case TILEDB_FLOAT64:
                return FormatTools.getPixelTypeString(FormatTools.DOUBLE);
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    public static long[] getMinMax(ByteBuffer buf, Datatype type) {
        if (!buf.hasRemaining()) {
            throw new IllegalArgumentException("Cannot get max of empty buffer");
        }
        switch(type) {
            case TILEDB_UINT8: {
                long max = buf.get() & 0xff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.get() & 0xff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case TILEDB_INT8: {
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
            case TILEDB_UINT16: {
                long max = buf.getShort() & 0xffff;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getShort() & 0xffff;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case TILEDB_INT16: {
                long max = buf.getShort();
                long min = max;
                while (buf.hasRemaining()) {
                    short next = buf.getShort();
                    min = next < min ? next : min;
                    max = next > max ? next : max;
                }
                return new long[] {(long) min, (long) max};
            }
            case TILEDB_UINT32:{
                long max = buf.getInt() & 0xffffffffl;
                long min = max;
                while(buf.hasRemaining()) {
                    long val = buf.getInt() & 0xffffffffl;
                    min = val < min ? val : min;
                    max = val > max ? val : max;
                }
                return new long[] {min, max};
            }
            case TILEDB_INT32: {
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

    public static int getBytesPerPixel(Datatype type) {
        switch (type) {
            case TILEDB_UINT8:
            case TILEDB_INT8:
                return 1;
            case TILEDB_UINT16:
            case TILEDB_INT16:
                return 2;
            case TILEDB_UINT32:
            case TILEDB_INT32:
                return 4;
            case TILEDB_UINT64:
            case TILEDB_INT64:
                return 8;
            default:
                throw new IllegalArgumentException("Attribute type " + type.toString() + " not supported");
        }
    }

    public static long[] getFullArrayDomain(Domain domain) throws TileDBError {
        int num_dims = (int) domain.getNDim();
        long[] subarrayDomain = new long[(int) num_dims*2];
        for(int i = 0; i < num_dims; i++) {
            if (domain.getDimension(i).getType() != Datatype.TILEDB_INT64) {
                throw new IllegalArgumentException("Dimension type "
                    + domain.getDimension(i).getType().toString() + " not supported");
            }
            long start = (long) (domain.getDimension(i).getDomain().getFirst());
            long end = (long) domain.getDimension(i).getDomain().getSecond();
            subarrayDomain[i*2] = start;
            subarrayDomain[i*2 + 1] = end;
        }
        return subarrayDomain;
    }

    public static long[] getSubarrayDomainFromString(String domainStr) {
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
        long[] subarrayDomain = new long[5*2];
        for(int i = 0; i < 5; i++) {
            String s = dimStrs[i];
            if(s.contains(":")) {
                String[] startEnd = s.split(":");
                subarrayDomain[i*2] = Long.valueOf(startEnd[0]);
                subarrayDomain[i*2 + 1] = Long.valueOf(startEnd[1]) - 1;
            } else {
                subarrayDomain[i*2] = Long.valueOf(s);
                subarrayDomain[i*2 + 1] = Long.valueOf(s);
            }
        }
        return subarrayDomain;
    }

    public static byte[] getBytes(String fullNgffPath) throws TileDBError {
        try (Context ctx = new Context();
                Array array = new Array(ctx, fullNgffPath, QueryType.TILEDB_READ)){
                    return TiledbUtils.getData(array, ctx);
        }
    }

    public static byte[] getBytes(String fullNgffPath, String domainStr, Integer maxTileLength) throws TileDBError {
        try (Context ctx = new Context();
                Array array = new Array(ctx, fullNgffPath, QueryType.TILEDB_READ)){
                    return TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
        }
    }

    public static void setupAwsConfig(Config config, String accessKey, String secretKey,
            String awsRegion, String s3EndpointOverride) throws TileDBError {
        config.set("vfs.s3.aws_access_key_id", accessKey);
        config.set("vfs.s3.aws_secret_access_key", secretKey);
        config.set("vfs.s3.scheme", "https");
        if(awsRegion != null) {
            config.set("vfs.s3.region", awsRegion);
        } else {
            config.set("vfs.s3.region", "us-east-1");
        }
        if(s3EndpointOverride != null) {
            config.set("vfs.s3.endpoint_override", s3EndpointOverride);
        }
        config.set("vfs.s3.use_virtual_addressing", "true");
    }

    public String getImageDataPath(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel) {
        if (ngffDir.startsWith("s3://")) {
            StringBuilder s3PathBuilder = new StringBuilder().append(ngffDir);
            if (!ngffDir.endsWith("/")) {
                s3PathBuilder.append("/");
            }
            s3PathBuilder.append(filesetId).append(".tiledb/")
                .append(series).append("/")
                .append(resolutionLevel);
            return s3PathBuilder.toString();
        } else {
            Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".tiledb").resolve(Integer.toString(series)).resolve(Integer.toString(resolutionLevel));
            return tiledbDataPath.toString();
        }
    }

    private String getS3LabelImagePath(String ngffDir, long filesetId, int series, String uuid, Integer resolution) {
        StringBuilder s3PathBuilder = new StringBuilder();
        s3PathBuilder.append(ngffDir);
        if(!s3PathBuilder.toString().endsWith("/")) {
            s3PathBuilder.append("/");
        }
        s3PathBuilder.append(filesetId).append(".tiledb").append("/")
            .append(series).append("/")
            .append("labels").append("/")
            .append(uuid).append("/")
            .append(resolution).append("/");
        return s3PathBuilder.toString();
    }

    private String getLocalLabelImagePath(String ngffDir, long filesetId, int series, String uuid, Integer resolution) {
        Path labelImageBasePath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                + ".tiledb/" + Integer.toString(series));
        Path labelImageLabelsPath = labelImageBasePath.resolve("labels");
        Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
        Path fullNgffDir = labelImageShapePath.resolve(Integer.toString(resolution));
        return fullNgffDir.toString();
    }

    public byte[] getLabelImageBytes(String ngffDir, long filesetId, int series, String uuid, Integer resolution,
            String domainStr) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_label_image_bytes");
        try(Config config = new Config()) {
            String ngffPath;
            if (ngffDir.startsWith("s3://")) {
                span.tag("location", "s3");
                setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
                ngffPath = getS3LabelImagePath(ngffDir, filesetId, series, uuid, resolution);
            } else {
                span.tag("location", "local");
                ngffPath = getLocalLabelImagePath(ngffDir, filesetId, series, uuid, resolution);
            }
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, ngffPath, QueryType.TILEDB_READ)){
                        return TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
            }
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

    public PixelData getImagePixelDataS3(String ngffDir, Long filesetId, Integer series, Integer resolution,
            String domainStr, Integer maxTileLength) throws TileDBError {
        String s3PathStr = getS3ImageDataPath(ngffDir, filesetId, series, resolution);
        return getPixelDataS3(s3PathStr, domainStr, maxTileLength, accessKey, secretKey, awsRegion, s3EndpointOverride);
    }

    public static byte[] getBytesS3(String ngffPath, String domainStr, Integer maxTileLength,
            String accessKey, String secretKey, String region, String s3EndpointOverride) throws TileDBError {
        try(Config config = new Config()) {
            setupAwsConfig(config, accessKey, secretKey, region, s3EndpointOverride);
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, ngffPath, QueryType.TILEDB_READ)){
                        return TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
            }
        }
    }

    public static PixelData getPixelDataS3(String ngffPath, String domainStr, Integer maxTileLength,
            String accessKey, String secretKey, String region, String s3EndpointOverride) throws TileDBError {
        try(Config config = new Config()) {
            setupAwsConfig(config, accessKey, secretKey, region, s3EndpointOverride);
            //First check that the file exists
            try (Context ctx = new Context(config);
                    VFS vfs = new VFS(ctx)) {
            }
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, ngffPath, QueryType.TILEDB_READ)){
                        byte[] buffer = TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
                        PixelData d = new PixelData(getPixelsType(array.getSchema().getAttribute("a1").getType()), ByteBuffer.wrap(buffer));
                        d.setOrder(ByteOrder.nativeOrder());
                        return d;
            }
        }
    }

    public PixelData getPixelData(String ngffPath, String domainStr, Integer maxTileLength) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_pixel_data_from_path");
        try(Config config = new Config()) {
            if (ngffPath.startsWith("s3://")) {
                setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            }
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, ngffPath, QueryType.TILEDB_READ)){
                        byte[] buffer = TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
                        PixelData d = new PixelData(getPixelsType(array.getSchema().getAttribute("a1").getType()), ByteBuffer.wrap(buffer));
                        d.setOrder(ByteOrder.nativeOrder());
                        return d;
            }
        } finally {
            span.finish();
        }
    }

    public PixelData getPixelData(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel,
            String domainStr) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_pixel_data_from_dir");
        String ngffPath = getImageDataPath(ngffDir, filesetId, series, resolutionLevel);
        try(Config config = new Config()) {
            if (ngffPath.startsWith("s3://")) {
                setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            }
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, ngffPath, QueryType.TILEDB_READ)){
                        byte[] buffer = TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
                        PixelData d = new PixelData(getPixelsType(array.getSchema().getAttribute("a1").getType()), ByteBuffer.wrap(buffer));
                        d.setOrder(ByteOrder.nativeOrder());
                        return d;
            }
        } finally {
            span.finish();
        }
    }

    public static void rescaleSubarrayDomain(long[] subarrayDomain, Domain domain) throws TileDBError {
        if(subarrayDomain[0] > (long) domain.getDimension("t").getDomain().getSecond() ||
                subarrayDomain[2] > (long) domain.getDimension("c").getDomain().getSecond() ||
                subarrayDomain[4] > (long) domain.getDimension("z").getDomain().getSecond() ||
                subarrayDomain[6] > (long) domain.getDimension("y").getDomain().getSecond() ||
                subarrayDomain[8] > (long) domain.getDimension("x").getDomain().getSecond()) {
            throw new IllegalArgumentException("Starting index exceeds image size");
        }
        subarrayDomain[1] = subarrayDomain[1] > (long) domain.getDimension("t").getDomain().getSecond() ?
                (long) domain.getDimension("t").getDomain().getSecond() : subarrayDomain[1];
        subarrayDomain[3] = subarrayDomain[3] > (long) domain.getDimension("c").getDomain().getSecond() ?
                (long) domain.getDimension("c").getDomain().getSecond() : subarrayDomain[3];
        subarrayDomain[5] = subarrayDomain[5] > (long) domain.getDimension("z").getDomain().getSecond() ?
                (long) domain.getDimension("z").getDomain().getSecond() : subarrayDomain[5];
        subarrayDomain[7] = subarrayDomain[7] > (long) domain.getDimension("y").getDomain().getSecond() ?
                (long) domain.getDimension("y").getDomain().getSecond() : subarrayDomain[7];
        subarrayDomain[9] = subarrayDomain[9] > (long) domain.getDimension("x").getDomain().getSecond() ?
                (long) domain.getDimension("x").getDomain().getSecond() : subarrayDomain[9];
    }

    public static byte[] getData(Array array, Context ctx) throws TileDBError {
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();

        int num_dims = (int) domain.getNDim();
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }

        long[] subarrayDomain = getFullArrayDomain(domain);
        return getTiledbBytes(array, ctx, subarrayDomain);
    }

    public static byte[] getData(Array array, Context ctx, String subarrayString, Integer maxTileLength) throws TileDBError {
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();

        int num_dims = (int) domain.getNDim();
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }
        long[] subarrayDomain = getSubarrayDomainFromString(subarrayString);
        log.info(Arrays.toString(subarrayDomain));
        if(subarrayDomain[7] - subarrayDomain[6] > maxTileLength ||
                subarrayDomain[9] - subarrayDomain[8] > maxTileLength) {
            throw new IllegalArgumentException("Tile size exceeds max size of " + Integer.toString(maxTileLength));
        }
        return getTiledbBytes(array, ctx, subarrayDomain);
    }

    private static byte[] getTiledbBytes(Array array, Context ctx, long[] subarrayDomain) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_tiledb_bytes");
        rescaleSubarrayDomain(subarrayDomain, array.getSchema().getDomain());
        int capacity = 1;
        for(int i = 0; i < 5; i++) {
            capacity *= ((int) (subarrayDomain[2*i + 1] - subarrayDomain[2*i] + 1));
        }

        Attribute attribute = array.getSchema().getAttribute("a1");
        int bytesPerPixel = TiledbUtils.getBytesPerPixel(attribute.getType());
        capacity *= bytesPerPixel;

        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
        buffer.order(ByteOrder.nativeOrder());
        //Dimensions in Dense Arrays must be the same type
        try (Query query = new Query(array, QueryType.TILEDB_READ);
                NativeArray subArray = new NativeArray(ctx, subarrayDomain, Datatype.TILEDB_INT64)){
            query.setSubarray(subArray);
            query.setBuffer("a1", buffer);
            query.submit();
            byte[] outputBytes = new byte[buffer.capacity()];
            buffer.get(outputBytes);
            return outputBytes;
        } finally {
            span.finish();
        }
    }

    public int getResolutionLevels(String ngffDir, Long filesetId, Integer series) {
        if (ngffDir.startsWith("s3://")) {
            StringBuilder parentPathBuidler = new StringBuilder().append(ngffDir);
            if(!ngffDir.endsWith("/")) {
                parentPathBuidler.append("/");
            }
            parentPathBuidler.append(filesetId).append(".tiledb").append("/")
                .append(series).append("/");
            try {
                return getResolutionLevelCountS3(parentPathBuidler.toString());
            } catch (TileDBError e) {
                log.error("Error getting s3 TileDB resolution level count for image fileset" + Long.toString(filesetId), e);
                return -1;
            }
        } else {
            Path tiledbSeriesPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".tiledb").resolve(Integer.toString(series));
            File[] directories = new File(tiledbSeriesPath.toString()).listFiles(File::isDirectory);
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

    public static long[] getMinMaxMetadata(Array array) throws TileDBError {
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

    public int getResolutionLevelCountS3(String parentPath) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_res_lvl_count_s3_path");
        try (Config config = new Config()) {
            setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, parentPath)) {
                String multiscalesMetaStr = TiledbUtils.getStringMetadata(array, "multiscales");
                JsonObject multiscales = new JsonObject(multiscalesMetaStr);
                JsonArray datasets = multiscales.getJsonArray("datasets");
                return datasets.size();
            }
        } finally {
            span.finish();
        }
    }

    public int getDimSize(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel, String dimName) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_dim_size");
        String tiledbPath;
        if (ngffDir.startsWith("s3://")) {
            span.tag("location", "s3");
            StringBuilder s3PathBuilder = new StringBuilder().append(ngffDir);
            if (!ngffDir.endsWith("/")) {
                s3PathBuilder.append("/");
            }
            s3PathBuilder.append(filesetId).append(".tiledb/")
                .append(series).append("/")
                .append(resolutionLevel);
            tiledbPath = s3PathBuilder.toString();
        } else {
            span.tag("location", "local");
            Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".tiledb").resolve(Integer.toString(series)).resolve(Integer.toString(resolutionLevel));
            tiledbPath = tiledbDataPath.toString();
        }
        try (Config config = new Config()) {
            if (ngffDir.startsWith("s3://")) {
                TiledbUtils.setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            }
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, tiledbPath, QueryType.TILEDB_READ)){
                Domain dom = array.getSchema().getDomain();
                return (int) ((long) dom.getDimension(dimName).getDomain().getSecond()
                        - (long) dom.getDimension(dimName).getDomain().getFirst() + 1);
            }
        } catch (TileDBError e) {
            log.error("TileDBError in getDimSize", e);
        } finally {
            span.finish();
        }
        return 0;
    }

    public Integer[] getSizeXandY(String ngffDir, Long filesetId, Integer series, Integer resolutionLevel) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_size_x_and_y");
        String tiledbPath;
        if (ngffDir.startsWith("s3://")) {
            span.tag("location", "s3");
            StringBuilder s3PathBuilder = new StringBuilder().append(ngffDir);
            if (!ngffDir.endsWith("/")) {
                s3PathBuilder.append("/");
            }
            s3PathBuilder.append(filesetId).append(".tiledb/")
                .append(series).append("/")
                .append(resolutionLevel);
            tiledbPath = s3PathBuilder.toString();
        } else {
            span.tag("location", "local");
            Path tiledbDataPath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".tiledb").resolve(Integer.toString(series)).resolve(Integer.toString(resolutionLevel));
            tiledbPath = tiledbDataPath.toString();
        }
        try (Config config = new Config()) {
            if (ngffDir.startsWith("s3://")) {
                TiledbUtils.setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            }
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, tiledbPath, QueryType.TILEDB_READ)){
                Domain dom = array.getSchema().getDomain();
                Integer[] xy = new Integer[2];
                xy[0] = (int) ((long) dom.getDimension("x").getDomain().getSecond()
                        - (long) dom.getDimension("x").getDomain().getFirst() + 1);
                xy[1] = (int) ((long) dom.getDimension("y").getDomain().getSecond()
                        - (long) dom.getDimension("y").getDomain().getFirst() + 1);
                return xy;
            }
        } catch (TileDBError e) {
            log.error("TileDBError in getDimSize", e);
            span.error(e);
        } finally {
            span.finish();
        }
        return null;
    }

    private static JsonObject getMetadataFromArray(Array array, long[] minMax,
            JsonObject multiscales, String uuid) throws TileDBError {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_metadata_from_array");
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();
        Attribute attribute = schema.getAttribute("a1");

        JsonObject metadata = new JsonObject();
        if(minMax != null) {
            metadata.put("min", minMax[0]);
            metadata.put("max", minMax[1]);
        }
        JsonObject size = new JsonObject();
        size.put("t", (long) domain.getDimension("t").getDomain().getSecond() -
                (long) domain.getDimension("t").getDomain().getFirst() + 1);
        size.put("c", (long) domain.getDimension("c").getDomain().getSecond() -
                (long) domain.getDimension("c").getDomain().getFirst() + 1);
        size.put("z", (long) domain.getDimension("z").getDomain().getSecond() -
                (long) domain.getDimension("z").getDomain().getFirst() + 1);
        size.put("width", (long) domain.getDimension("x").getDomain().getSecond() -
                (long) domain.getDimension("x").getDomain().getFirst() + 1);
        size.put("height", (long) domain.getDimension("y").getDomain().getSecond() -
                (long) domain.getDimension("y").getDomain().getFirst() + 1);
        metadata.put("size", size);
        metadata.put("type", attribute.getType().toString());
        if(multiscales != null) {
            metadata.put("multiscales", multiscales);
        }
        metadata.put("uuid", uuid);
        span.finish();
        return metadata;
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
        if (ngffDir.startsWith("s3://")) {
            return getLabelImageMetadataS3(ngffDir, filesetId, series, uuid, resolution);
        } else {
            return getLabelImageMetadataLocal(ngffDir, filesetId, series, uuid, resolution);
        }
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
        Path labelImageBasePath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                + ".tiledb").resolve(Integer.toString(series));
        Path labelImageLabelsPath = labelImageBasePath.resolve("labels");
        Path labelImageShapePath = labelImageLabelsPath.resolve(uuid);
        Path fullngffDir = labelImageShapePath.resolve(Integer.toString(resolution));
        JsonObject multiscales = null;
        long[] minMax = null;
        if (Files.exists(fullngffDir)) {
            try (Context ctx = new Context();
                Array array = new Array(ctx, labelImageShapePath.toString(), QueryType.TILEDB_READ)) {
                if(array.hasMetadataKey("multiscales")) {
                    String multiscalesMetaStr = TiledbUtils.getStringMetadata(array, "multiscales");
                    minMax = getMinMaxMetadata(array);
                    multiscales = new JsonObject(multiscalesMetaStr);
                }
            } catch (Exception e) {
                log.error("Exception while retrieving label image metadata", e);
            }
            try (Context ctx = new Context();
                Array array = new Array(ctx, fullngffDir.toString(), QueryType.TILEDB_READ)){
                return getMetadataFromArray(array, minMax, multiscales, uuid);
            } catch (Exception e) {
                log.error("Exception while retrieving label image metadata", e);
            } finally {
                span.finish();
            }
        }
        return null;
    }


    /**
     * Get label image metadata for S3 label image.
     * @param ngffDir The base directory for ngff data
     * @param filesetId The fileset ID of the image
     * @param series The series ID of the image in the fileset
     * @param uuid The External Info UUID of the shape associated with the label image
     * @param ngffDir the base directory for ngff data
     * @return A JsonObject with the label image metadata
     */
    private JsonObject getLabelImageMetadataS3(String ngffDir, long filesetId, int series, String uuid, int resolution) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("get_label_image_metadata_s3");
        ScopedSpan configSpan = Tracing.currentTracer().startScopedSpan("get_label_image_metadata_s3_config");
        StringBuilder s3PathBuilder = new StringBuilder();
        s3PathBuilder.append(ngffDir);
        if(!s3PathBuilder.toString().endsWith("/")) {
            s3PathBuilder.append("/");
        }
        s3PathBuilder.append(filesetId).append(".tiledb").append("/")
            .append(series).append("/")
            .append("labels").append("/")
            .append(uuid);
        String labelImageShapePath = s3PathBuilder.toString();
        s3PathBuilder.append("/")
            .append(resolution).append("/");
        String fullNgffPath = s3PathBuilder.toString();
        JsonObject multiscales = null;
        long[] minMax = null;

        try(Config config = new Config()) {
            setupAwsConfig(config, accessKey, secretKey, awsRegion, s3EndpointOverride);
            configSpan.finish();
            ScopedSpan arraySpan = Tracing.currentTracer().startScopedSpan("setup_label_shape_array_s3");
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, labelImageShapePath, QueryType.TILEDB_READ)){
                arraySpan.finish();
                arraySpan = Tracing.currentTracer().startScopedSpan("get_multiscales_minmax_metadata");
                if(array.hasMetadataKey("multiscales")) {
                    String multiscalesMetaStr = TiledbUtils.getStringMetadata(array, "multiscales");
                    minMax = getMinMaxMetadata(array);
                    multiscales = new JsonObject(multiscalesMetaStr);
                }
                arraySpan.finish();
            } catch (Exception e) {
                log.error("Exception while retrieving label image metadata", e);
            }
            arraySpan = Tracing.currentTracer().startScopedSpan("setup_label_image_data_array_s3");
            try (Context ctx = new Context(config);
                Array array = new Array(ctx, fullNgffPath.toString(), QueryType.TILEDB_READ)){
                arraySpan.finish();
                return getMetadataFromArray(array, minMax, multiscales, uuid);
            } catch (Exception e) {
                log.error("Exception while retrieving label image metadata", e);
                span.error(e);
            } finally {
                span.finish();
            }
        } catch (TileDBError e) {
            log.error("TiledbError when trying to get metadata from S3", e);
            span.error(e);
            span.finish();
        }
        return null;
    }
}
