/*
 * Copyright (C) 2025 Glencoe Software, Inc. All rights reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonObject;
import ome.io.nio.FileBuffer;
import ome.io.nio.OriginalFilesService;
import ome.model.core.OriginalFile;
import omero.RLong;
import omero.RString;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.FileAnnotationI;
import omero.model.MaskI;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class AnnotationRequestHandler {

    private static final org.slf4j.Logger log = LoggerFactory
            .getLogger(AnnotationRequestHandler.class);

    AnnotationCtx annotationCtx;

    /**
     * Constructor
     * @param annotationCtx
     */
    public AnnotationRequestHandler(
            AnnotationCtx annotationCtx) {
        this.annotationCtx = annotationCtx;
    }

    /**
     * Queries the database for the file metadata assiciated with the
     * {@link OriginalFile} associated with the given {@link FileAnnotation}
     * @param client OMERO client
     * @return JsonObject with the {@link OriginalFile} ID and file name if
     * present, otherwise null
     */
    public JsonObject getFileIdAndNameForAnnotation(omero.client client) {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(annotationCtx.annotationId);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan(
                        "get_file_id_and_name");
        try {
            List<List<RType>> rows = client.getSession()
                .getQueryService().projection(
                        "SELECT ann.file.id, ann.file.name FROM "
                        + "Annotation ann WHERE ann.id = :id",
                        params, ctx);
            if (rows.isEmpty()) {
                return null;
            }
            JsonObject retObj = new JsonObject();
            retObj.put("originalFileId", ((RLong) rows.get(0).get(0)).getValue());
            retObj.put("originalFileName", ((RString) rows.get(0).get(1)).getValue());
            return retObj;
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while checking annotation readability", e);
        } finally {
            span.finish();
        }
        return null;
    }
}
