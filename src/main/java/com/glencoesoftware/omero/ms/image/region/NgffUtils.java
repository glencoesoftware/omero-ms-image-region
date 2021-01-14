package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.LoggerFactory;

import io.tiledb.java.api.TileDBError;
import io.vertx.core.json.JsonObject;

public class NgffUtils {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(NgffUtils.class);

    private final TiledbUtils tiledbUtils;
    private final OmeroZarrUtils zarrUtils;

    public NgffUtils(TiledbUtils tiledbUtils, OmeroZarrUtils zarrUtils) {
        this.tiledbUtils = tiledbUtils;
        this.zarrUtils = zarrUtils;
    }

    public byte[] getLabelImageBytes(String ngffDir, long filesetId, int series, String uuid, Integer resolution,
            String domainStr) throws TileDBError {
        if(ngffDir.startsWith("s3://")) {
            //TODO Use AWS CLI to check file type (list with prefix?)
            return null;
        } else {
            Path ngffRoot = Paths.get(ngffDir, Long.toString(filesetId) + ".tiledb");
            if(Files.exists(ngffRoot)) {
                return tiledbUtils.getLabelImageBytes(ngffDir, filesetId, series, uuid, resolution, domainStr);
            }
            ngffRoot = Paths.get(ngffDir, Long.toString(filesetId) + ".zarr");
            if(Files.exists(ngffRoot) ) {
                return zarrUtils.getLabelImageBytes(ngffDir, filesetId, series, uuid, resolution, domainStr);
            }
            log.error("Ngff file missing or unsupported type: ", ngffDir, filesetId);
            return null;
        }
    }

    public JsonObject getLabelImageMetadata(String ngffDir, long filesetId, int series, String uuid, int resolution) {
        Path basePath;
        try {
            basePath = zarrUtils.getLocalOrS3Path(ngffDir);
        } catch (IOException e) {
            log.error("Error connecting to S3", e);
            return null;
        }
        Path ngffRoot = basePath.resolve(Long.toString(filesetId) + ".tiledb");
        if(Files.exists(ngffRoot)) {
            return tiledbUtils.getLabelImageMetadata(ngffDir, filesetId, series, uuid, resolution);
        }
        ngffRoot = basePath.resolve(Long.toString(filesetId) + ".zarr");
        if(Files.exists(ngffRoot) ) {
            return zarrUtils.getLabelImageMetadata(ngffDir, filesetId, series, uuid, resolution);
        }
        log.error("Ngff file missing or unsupported type: " + ngffDir + " "+ Long.toString(filesetId));
        return null;
    }

}
