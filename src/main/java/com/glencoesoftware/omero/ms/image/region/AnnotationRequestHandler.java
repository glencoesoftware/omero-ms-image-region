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
import ome.io.nio.FileBuffer;
import ome.io.nio.OriginalFilesService;
import ome.model.core.OriginalFile;
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

    OriginalFilesService ioService;

    /**
     * Constructor
     * @param annotationCtx
     */
    public AnnotationRequestHandler(
            AnnotationCtx annotationCtx,
            OriginalFilesService ioService) {
        this.annotationCtx = annotationCtx;
        this.ioService = ioService;
    }

    public String getFileAnnotationPath(omero.client client) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_annotation");
        try {
            if (!RequestHandlerUtils.canRead(client, "Annotation",
                    annotationCtx.annotationId)) {
                return null;
            }
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            FileAnnotationI fileAnnotation = (FileAnnotationI)
                    iQuery.get("FileAnnotation", annotationCtx.annotationId);
            OriginalFile of = (OriginalFile) new IceMapper().reverse(
                    iQuery.get("OriginalFile",
                            fileAnnotation.getFile().getId().getValue()));
            FileBuffer fBuffer = ioService.getFileBuffer(of, "r");
            log.info(fBuffer.getPath());
            return fBuffer.getPath();
        } catch (ServerError e) {
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Whether or not a single {@link MaskI} can be read from the server.
     * @param client OMERO client to use for querying.
     * @return <code>true</code> if the {@link Mask} can be loaded or
     * <code>false</code> otherwise.
     * @throws ServerError If there was any sort of error retrieving the image.
     */
    public boolean canRead(omero.client client) {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(annotationCtx.annotationId);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("can_read");
        try {
            List<List<RType>> rows = client.getSession()
                    .getQueryService().projection(
                            "SELECT ann.id FROM Annotation as ann " +
                            "WHERE ann.id = :id", params, ctx);
            if (rows.size() > 0) {
                return true;
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while checking annotation readability", e);
        } finally {
            span.finish();
        }
        return false;
    }
}
