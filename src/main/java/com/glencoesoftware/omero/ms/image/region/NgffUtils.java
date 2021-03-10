package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.LoggerFactory;

import io.tiledb.java.api.TileDBError;
import io.vertx.core.json.JsonObject;

public class NgffUtils {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(NgffUtils.class);

    private static final String ZARR_EXTN = OmeroZarrUtils.ZARR_EXTN;
    private static final String TILEDB_EXTN = TiledbUtils.TILEDB_EXTN;

    /** TiledbUtils */
    private final TiledbUtils tiledbUtils;

    /** ZarrUtils */
    private final OmeroZarrUtils zarrUtils;

    /**
     * Default Constructor
     * @param tiledbUtils Configured TiledbUtils
     * @param zarrUtils Configured ZarrUtils
     */
    public NgffUtils(TiledbUtils tiledbUtils, OmeroZarrUtils zarrUtils) {
        this.tiledbUtils = tiledbUtils;
        this.zarrUtils = zarrUtils;
    }

    /**
     * Get byte array of data for label image
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param uuid Shape UUID
     * @param resolution Requested resolution level
     * @param domainStr String like [0,1,0,100:150,200:250] representing the
     * region to retrieve
     * @return A byte array containing the label image data for the requested
     * region
     * @throws TileDBError
     */
    public byte[] getLabelImageBytes(
        String ngffDir, long filesetId, int series, String uuid,
        Integer resolution, String domainStr)
                throws TileDBError {
        Path basePath;
        try {
            basePath = zarrUtils.getLocalOrS3Path(ngffDir);
        } catch (IOException e) {
            log.error("Error connecting to S3", e);
            return null;
        }
        Path ngffRoot = basePath.resolve(Long.toString(filesetId) + ZARR_EXTN);
        if (Files.exists(ngffRoot) ) {
            return zarrUtils.getLabelImageBytes(
                    ngffDir, filesetId, series, uuid, resolution, domainStr);
        }
        ngffRoot = basePath.resolve(Long.toString(filesetId) + TILEDB_EXTN);
        if (Files.exists(ngffRoot)) {
            return tiledbUtils.getLabelImageBytes(
                    ngffDir, filesetId, series, uuid, resolution, domainStr);
        }
        log.error(
            "Ngff file missing or unsupported type: ", ngffDir, filesetId);
        return null;
    }

    /**
     * Get label image metadata
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @param uuid Shape UUID
     * @param resolution Requested resolution level
     * @return A JsonObject containing the label image metadata
     */
    public JsonObject getLabelImageMetadata(
            String ngffDir, long filesetId, int series, String uuid,
            int resolution) {
        Path basePath;
        try {
            basePath = zarrUtils.getLocalOrS3Path(ngffDir);
        } catch (IOException e) {
            log.error("Error connecting to S3", e);
            return null;
        }
        Path ngffRoot = basePath.resolve(Long.toString(filesetId) + ZARR_EXTN);
        if (Files.exists(ngffRoot) ) {
            return zarrUtils.getLabelImageMetadata(
                    ngffDir, filesetId, series, uuid, resolution);
        }
        ngffRoot = basePath.resolve(Long.toString(filesetId) + TILEDB_EXTN);
        if (Files.exists(ngffRoot)) {
            return tiledbUtils.getLabelImageMetadata(
                    ngffDir, filesetId, series, uuid, resolution);
        }
        log.error(
            "Ngff file missing or unsupported type: " +
            ngffDir + " "+ Long.toString(filesetId));
        return null;
    }

    /**
     * Get 'omero' metadata from the image
     * @param ngffDir Top-level directory containing NGFF files
     * @param filesetId Fileset ID
     * @param series Series
     * @return JsonObject containing 'omero' metadata
     */
    public JsonObject getOmeroMetadata(
            String ngffDir, long filesetId, int series) {
        Path basePath;
        try {
            basePath = zarrUtils.getLocalOrS3Path(ngffDir);
        } catch (IOException e) {
            log.error("Error connecting to S3", e);
            return null;
        }
        Path ngffRoot = basePath.resolve(Long.toString(filesetId) + ZARR_EXTN);
        if (Files.exists(ngffRoot) ) {
            return zarrUtils.getOmeroMetadata(ngffDir, filesetId, series);
        }
        ngffRoot = basePath.resolve(Long.toString(filesetId) + TILEDB_EXTN);
        if (Files.exists(ngffRoot)) {
            return tiledbUtils.getOmeroMetadata(ngffDir, filesetId, series);
        }

        log.error(
            "Ngff file missing or unsupported type: " +
            ngffDir + " "+ Long.toString(filesetId));
        return null;
    }
}
