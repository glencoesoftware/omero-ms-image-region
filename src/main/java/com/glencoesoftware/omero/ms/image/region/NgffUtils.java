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
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public class NgffUtils {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(NgffUtils.class);

    private static final String ZARR_EXTN = OmeroZarrUtils.ZARR_EXTN;

    /** ZarrUtils */
    private final OmeroZarrUtils zarrUtils;

    /**
     * Default Constructor
     * @param tiledbUtils Configured TiledbUtils
     * @param zarrUtils Configured ZarrUtils
     */
    public NgffUtils(OmeroZarrUtils zarrUtils) {
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
        Integer resolution, String domainStr) {
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
        log.error(
            "Ngff file missing or unsupported type: {} {}", ngffDir, filesetId);
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
        log.error(
            "Ngff file missing or unsupported type: {} {}",
            ngffDir, filesetId);
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
        log.error(
            "Ngff file missing or unsupported type: {} {}",
            ngffDir, filesetId);
        return null;
    }
}
