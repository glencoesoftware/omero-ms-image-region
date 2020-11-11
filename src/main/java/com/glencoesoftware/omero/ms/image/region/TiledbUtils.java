package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.LoggerFactory;

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
import io.vertx.core.json.JsonObject;
import omero.model.Image;
import omero.model.MaskI;

public class TiledbUtils {

    private static final org.slf4j.Logger log =
        LoggerFactory.getLogger(ShapeMaskRequestHandler.class);

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

    public static byte[] getBytesS3(String fullNgffPath, String accessKey,
            String secretKey, String region) throws TileDBError {
        try (Context ctx = new Context();
                Array array = new Array(ctx, fullNgffPath, QueryType.TILEDB_READ)){
                    return TiledbUtils.getData(array, ctx);
        }
    }

    public static byte[] getBytesS3(String fullNgffPath, String domainStr, Integer maxTileLength,
            String accessKey, String secretKey, String s3EndpointOverride) throws TileDBError {
        log.info(fullNgffPath);
        log.info(domainStr);
        log.info(accessKey);
        log.info(secretKey);
        log.info(s3EndpointOverride);
        log.info(s3EndpointOverride.split("\\.")[0]);
        try(Config config = new Config()) {
            config.set("vfs.s3.aws_access_key_id", accessKey);
            config.set("vfs.s3.aws_secret_access_key", secretKey);
            //config.set("vfs.s3.aws_secret_access_key", "abc");
            config.set("vfs.s3.scheme", "https");
            config.set("vfs.s3.region", "us-east-1");
            config.set("vfs.s3.endpoint_override", s3EndpointOverride);
            config.set("vfs.s3.use_virtual_addressing", "true");
            try (Context ctx = new Context(config);
                    Array array = new Array(ctx, fullNgffPath, QueryType.TILEDB_READ)){
                        return TiledbUtils.getData(array, ctx, domainStr, maxTileLength);
            }
        }
        /*
        try(Config config = new Config()) {
            config.set("vfs.s3.aws_access_key_id", accessKey);
            config.set("vfs.s3.aws_secret_access_key", secretKey);
            //config.set("vfs.s3.aws_secret_access_key", "abc");
            config.set("vfs.s3.scheme", "https");
            //config.set("vfs.s3.region", s3EndpointOverride.split("\\.")[0]);
            config.set("vfs.s3.region", "us-east-1");
            config.set("vfs.s3.endpoint_override", s3EndpointOverride);
            config.set("vfs.s3.use_virtual_addressing", "true");
            try (Context ctx = new Context(config);
                    VFS vfs = new VFS(ctx)){
                log.info(Boolean.toString(vfs.isBucket("s3://zarr-bucket")));
            }
        }
        */
    }

    public static byte[] getData(Array array, Context ctx) throws TileDBError {
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();
        Attribute attribute = schema.getAttribute("a1");

        int num_dims = (int) domain.getNDim();
        if (num_dims != 5) {
            throw new IllegalArgumentException("Number of dimensions must be 5. Actual was: "
                    + Integer.toString(num_dims));
        }

        long[] subarrayDomain = getFullArrayDomain(domain);
        int capacity = 1;
        for(int i = 0; i < subarrayDomain.length/2; i++) {
            capacity *= subarrayDomain[i*2 + 1] - subarrayDomain[i*2] + 1;
        }
        int bytesPerPixel = TiledbUtils.getBytesPerPixel(attribute.getType());
        capacity *= bytesPerPixel;
        log.info(Integer.toString(capacity));

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
        }
    }

    public static void rescaleSubarrayDomain(long[] subarrayDomain, Domain domain) throws TileDBError {
        if(subarrayDomain[0] > (long) domain.getDimension("t").getDomain().getFirst() ||
                subarrayDomain[2] > (long) domain.getDimension("c").getDomain().getFirst() ||
                subarrayDomain[4] > (long) domain.getDimension("z").getDomain().getFirst() ||
                subarrayDomain[6] > (long) domain.getDimension("y").getDomain().getFirst() ||
                subarrayDomain[8] > (long) domain.getDimension("x").getDomain().getFirst()) {
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

    public static byte[] getData(Array array, Context ctx, String subarrayString, Integer maxTileLength) throws TileDBError {
        ArraySchema schema = array.getSchema();
        Domain domain = schema.getDomain();
        Attribute attribute = schema.getAttribute("a1");

        int bytesPerPixel = TiledbUtils.getBytesPerPixel(attribute.getType());

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
        //Rescale subdomain to not exceed image bounds
        rescaleSubarrayDomain(subarrayDomain, domain);
        int capacity = 1;
        for(int i = 0; i < 5; i++) {
            capacity *= ((int) (subarrayDomain[2*i + 1] - subarrayDomain[2*i] + 1));
        }
        capacity *= bytesPerPixel;
        log.info(Integer.toString(capacity));

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
        }
    }

    public static String getStringMetadata(Array array, String key) throws TileDBError {
        if(array.hasMetadataKey(key)) {
            NativeArray strNativeArray = array.getMetadata(key, Datatype.TILEDB_CHAR);
            return new String((byte[]) strNativeArray.toJavaArray(), StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    public static long[] getMinMaxMetadata(Array array) throws TileDBError {
        String key = "minmax";
        log.info(array.getMetadataMap().keySet().toString());
        if(array.hasMetadataKey(key)) {
            NativeArray minMaxNativeArray = array.getMetadata(key, Datatype.TILEDB_INT32);
            log.info(minMaxNativeArray.getJavaType().toString());
            return (long[]) minMaxNativeArray.toJavaArray();
        } else {
            return null;
        }
    }

    public static int getResolutionLevelCount(Path labelImageShapePath) {
        File[] directories = new File(labelImageShapePath.toString()).listFiles(File::isDirectory);
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

    /**
     * Get shape mask bytes request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>shapeMaskCtx</code>.
     */
    public static JsonObject getLabelImageMetadata(String ngffDir, long filesetId, int series, String uuid, int resolution) {
            Path labelImageBasePath = Paths.get(ngffDir).resolve(Long.toString(filesetId)
                    + ".tiledb/" + Integer.toString(series));
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
                    metadata.put("levels", getResolutionLevelCount(labelImageShapePath));
                    return metadata;
                } catch (Exception e) {
                    log.error("Exception while retrieving label image metadata", e);
                }
            } else {
                return null;
            }
        return null;
    }
}
