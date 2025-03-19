/*
 * Copyright (C) 2017 Glencoe Software, Inc. All rights reserved.
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

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.OmeroMsAbstractVerticle;
import com.glencoesoftware.omero.ms.core.OmeroRequest;
import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;
import com.glencoesoftware.omero.zarr.ZarrPixelsService;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.TraceContext;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.api.IScale;
import ome.api.local.LocalCompress;
import ome.io.nio.OriginalFilesService;
import omeis.providers.re.lut.LutProvider;
import omero.ApiUsageException;
import omero.ServerError;
import omero.model.Image;
import omero.util.IceMapper;

public class ImageRegionVerticle extends OmeroMsAbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionVerticle.class);

    public static final String RENDER_IMAGE_REGION_EVENT =
            "omero.render_image_region";

    public static final String RENDER_THUMBNAIL_EVENT =
            "omero.render_thumbnail";

    public static final String GET_THUMBNAILS_EVENT =
            "omero.get_thumbnails";

    public static final String GET_IMAGE_DATA_EVENT =
            "omero.get_image_data";

    public static final String GET_HISTOGRAM_JSON_EVENT =
            "omero.get_histogram_json";

    public static final String GET_FILE_ANNOTATION_METADATA_EVENT =
            "omero.get_file_annotation";

    /** OMERO server host */
    private String host;

    /** OMERO server port */
    private int port;

    /** Lookup table provider. */
    private final LutProvider lutProvider;

    /** Available families */
    private List<Family> families;

    /** Available rendering models */
    private List<RenderingModel> renderingModels;

    /** Reference to the compression service */
    private final LocalCompress compressionService;

    /** Configured maximum size size in either dimension */
    private final int maxTileLength;

    private final ZarrPixelsService pixelsService;

    /** Scaling service for thumbnails */
    private final IScale iScale;

    /** Original File Service for getting paths */
    private OriginalFilesService ioService;

    /**
     * Default constructor.
     */
    public ImageRegionVerticle(
            LocalCompress compressionService,
            LutProvider lutProvider,
            int maxTileLength,
            ZarrPixelsService pixelsService,
            IScale iScale,
            OriginalFilesService ioService)
    {
        this.compressionService = compressionService;
        this.lutProvider = lutProvider;
        this.maxTileLength = maxTileLength;
        this.pixelsService = pixelsService;
        this.iScale = iScale;
        this.ioService = ioService;
    }

    /* (non-Javadoc)
     * @see io.vertx.core.Verticle#start(io.vertx.core.Promise)
     */
    @Override
    public void start(Promise<Void> startPromise) {
        try {
            JsonObject omero = config().getJsonObject("omero");
            if (omero == null) {
                throw new IllegalArgumentException(
                        "'omero' block missing from configuration");
            }
            host = omero.getString("host");
            port = omero.getInteger("port");
            vertx.eventBus().<String>consumer(
                    RENDER_IMAGE_REGION_EVENT, event -> {
                        renderImageRegion(event);
                    });
            vertx.eventBus().<String>consumer(
                    RENDER_THUMBNAIL_EVENT, this::renderThumbnail);
            vertx.eventBus().<String>consumer(
                    GET_THUMBNAILS_EVENT, this::getThumbnails);
            vertx.eventBus().<String>consumer(
                    GET_IMAGE_DATA_EVENT, this::getImageData);
            vertx.eventBus().<String>consumer(
                    GET_HISTOGRAM_JSON_EVENT, this::getHistogramJson);
            vertx.eventBus().<String>consumer(
                    GET_FILE_ANNOTATION_METADATA_EVENT, this::getFileAnnotationMetadata);
        } catch (Exception e) {
            startPromise.fail(e);
        }
        startPromise.complete();
    }

    /**
     * Render Image region event handler. Responds with a
     * request body on success based on the <code>format</code>
     * <code>imageId</code>, <code>z</code> and <code>t</code> encoded in the
     * URL or HTTP 404 if the {@link Image} does not exist or the user
     * does not have permissions to access it.
     * @param event Current routing context.
     * @param message JSON encoded {@link ImageRegionCtx} object.
     */
    private void renderImageRegion(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageRegionCtx;
        try {
            String body = message.body();
            imageRegionCtx = mapper.readValue(body, ImageRegionCtx.class);
        } catch (Exception e) {
            String v = "Illegal image region context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        TraceContext traceCtx = extractor().extract(
                imageRegionCtx.traceContext).context();
        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "handle_render_image_region",
                traceCtx);
        span.tag("ctx", message.body());

        try (OmeroRequest request = new OmeroRequest(
                 host, port, imageRegionCtx.omeroSessionKey))
        {
            if (families == null) {
                request.execute(this::updateFamilies);
            }
            if (renderingModels == null) {
                request.execute(this::updateRenderingModels);
            }
            byte[] imageRegion = null;
            imageRegion = request.execute(
                    new ImageRegionRequestHandler(imageRegionCtx,
                            families,
                            renderingModels,
                            lutProvider,
                            compressionService,
                            maxTileLength,
                            pixelsService)::renderImageRegion);
            span.finish();
            if (imageRegion == null) {
                message.fail(
                        404, "Cannot find Image:" + imageRegionCtx.imageId);
            } else {
                message.reply(imageRegion);
            }
        } catch (PermissionDeniedException
                | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            span.error(e);
            message.fail(403, v);
        } catch (IllegalArgumentException e) {
            log.debug(
                "Illegal argument received while retrieving image region", e);
            span.error(e);
            message.fail(400, e.getMessage());
        } catch (Exception e) {
            String v = "Exception while retrieving image region";
            log.error(v, e);
            span.error(e);
            message.fail(500, v);
        }
    }

    /**
     * Render thumbnail event handler. Responds with a <code>image/jpeg</code>
     * body on success or a failure.
     * @param message JSON encoded event data. Required keys are
     * <code>omeroSessionKey</code> (String), <code>longestSide</code>
     * (Integer), and <code>imageId</code> (Long).
     */
    private void renderThumbnail(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ThumbnailCtx thumbnailCtx;
        log.info(message.body());
        try {
            thumbnailCtx = mapper.readValue(message.body(), ThumbnailCtx.class);
        } catch (Exception e) {
            String v = "Illegal tile context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }

        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "render_thumbnail",
                extractor().extract(thumbnailCtx.traceContext).context());
        String omeroSessionKey = thumbnailCtx.omeroSessionKey;
        log.debug(
            "Render thumbnail request: {}", thumbnailCtx.toString());

        try (OmeroRequest request = new OmeroRequest(
                 host, port, omeroSessionKey)) {
            if (families == null) {
                request.execute(this::updateFamilies);
            }
            if (renderingModels == null) {
                request.execute(this::updateRenderingModels);
            }
            byte[] thumbnail = request.execute(
                new ThumbnailsRequestHandler(
                        thumbnailCtx,
                        families,
                        renderingModels,
                        lutProvider,
                        compressionService,
                        maxTileLength,
                        pixelsService,
                        iScale)::renderThumbnail);
            if (thumbnail == null) {
                message.fail(
                        404, "Cannot find Images:" + thumbnailCtx.imageIds);
            } else {
                message.reply(thumbnail);
            }
        } catch (PermissionDeniedException
                 | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            message.fail(403, v);
        } catch (Exception e) {
            String v = "Exception while retrieving thumbnail";
            log.error(v, e);
            message.fail(500, v);
        } finally {
            span.finish();
        }
    }

    /**
     * Get thumbnails event handler. Responds with a JSON dictionary of Base64
     * encoded <code>image/jpeg</code> thumbnails keyed by {@link Image}
     * identifier. Each dictionary value is prefixed with
     * <code>data:image/jpeg;base64,</code> so that it can be used with
     * <a href="http://caniuse.com/#feat=datauri">data URIs</a>.
     * @param message JSON encoded event data. Required keys are
     * <code>omeroSessionKey</code> (String), <code>longestSide</code>
     * (Integer), and <code>imageIds</code> (List<Long>).
     */
    private void getThumbnails(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ThumbnailCtx thumbnailCtx;
        try {
            thumbnailCtx = mapper.readValue(message.body(), ThumbnailCtx.class);
        } catch (Exception e) {
            String v = "Illegal tile context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "get_thumbnails",
                extractor().extract(thumbnailCtx.traceContext).context());
        String omeroSessionKey = thumbnailCtx.omeroSessionKey;
        log.debug("Render thumbnail request: {}", thumbnailCtx.toString());

        try (OmeroRequest request = new OmeroRequest(
                host, port, omeroSessionKey)) {
            if (families == null) {
                request.execute(this::updateFamilies);
            }
            if (renderingModels == null) {
                request.execute(this::updateRenderingModels);
            }
            Map<Long, byte[]> thumbnails = request.execute(
                    new ThumbnailsRequestHandler(
                            thumbnailCtx,
                            families,
                            renderingModels,
                            lutProvider,
                            compressionService,
                            maxTileLength,
                            pixelsService,
                            iScale)::renderThumbnails);

            if (thumbnails == null) {
                message.fail(404, "Cannot find one or more Images");
            } else {
                Map<Long, String> thumbnailsJson = new HashMap<Long, String>();
                for (Entry<Long, byte[]> v : thumbnails.entrySet()) {
                    String asBase64 =
                            Base64.getEncoder().encodeToString(v.getValue());
                    thumbnailsJson.put(
                        v.getKey(), "data:image/jpeg;base64," + asBase64
                    );
                }
                message.reply(Json.encode(thumbnailsJson));
            }
        } catch (PermissionDeniedException
                 | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            message.fail(403, v);
        } catch (Exception e) {
            String v = "Exception while retrieving thumbnail";
            log.error(v, e);
            message.fail(500, v);
        } finally {
            span.finish();
        }
    }

    /**
     * Get image data event handler. Responds with a JSON string of image
     * metadata.
     * @param message JSON encoded event data. Required keys are
     * <code>omeroSessionKey</code> (String) and <code>imageId</code>
     */
    private void getImageData(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ImageDataCtx imgDataCtx;
        try {
            imgDataCtx = mapper.readValue(message.body(), ImageDataCtx.class);
        } catch (Exception e) {
            String v = "Illegal image data context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "get_image_data",
                extractor().extract(imgDataCtx.traceContext).context());
        String omeroSessionKey = imgDataCtx.omeroSessionKey;
        log.debug("Get image data request: {}", imgDataCtx.toString());
        JsonObject omeroServer = config().getJsonObject("omero.server");
        int init_zoom = Integer.valueOf(omeroServer.getString("omero.client.viewer.initial_zoom_level", "0"));
        boolean interpolate = Boolean.valueOf(omeroServer.getString("omero.client.viewer.interpolate_pixels", "true"));
        try (OmeroRequest request = new OmeroRequest(
                host, port, omeroSessionKey)) {
                JsonObject imgData = request.execute(
                    new ImageDataRequestHandler(imgDataCtx,
                            pixelsService,
                            init_zoom,
                            interpolate)::getImageData);
                if (imgData == null) {
                    message.fail(404, "Cannot find the Image");
                }
                message.reply(imgData);
        } catch (PermissionDeniedException
                 | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            message.fail(403, v);
        } catch (Exception e) {
            String v = "Exception while getting image data";
            log.error(v, e);
            message.fail(500, v);
        } finally {
            span.finish();
        }
    }

    private void getHistogramJson(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        HistogramCtx histogramCtx;
        try {
            histogramCtx = mapper.readValue(message.body(), HistogramCtx.class);
        } catch (Exception e) {
            String v = "Illegal image region context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "get_histogram",
                extractor().extract(histogramCtx.traceContext).context());
        String omeroSessionKey = histogramCtx.omeroSessionKey;
        log.debug("Get histogram request: {}", histogramCtx.toString());
        String cacheKey = histogramCtx.cacheKey();
        vertx.eventBus().<byte[]>request(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, cacheKey, result -> {
                    try (OmeroRequest request = new OmeroRequest(host, port,
                            histogramCtx.omeroSessionKey)) {
                        byte[] histogramDataBytes = result.succeeded() ?
                                result.result().body() : null;
                        String histogramDataStr = null;
                        if (histogramDataBytes != null) {
                            log.info("Histogram in cache");
                            histogramDataStr = new String(histogramDataBytes);
                        }
                        HistogramRequestHandler requestHandler =
                                new HistogramRequestHandler(histogramCtx,
                                        pixelsService);

                        // If the histogram is in the cache, check we have permissions
                        // to access it and assign and return
                        if (histogramDataStr != null
                                && request.execute(requestHandler::canRead)) {
                            span.finish();
                            message.reply(new JsonObject(histogramDataStr));
                            log.info("Got histogram from cache!");
                            return;
                        }

                        JsonObject histogramData = request.execute(
                                requestHandler::getHistogramJson);
                        span.finish();
                        if (histogramData == null) {
                            message.fail(404, "Cannot find the Image");
                        }
                        message.reply(histogramData);

                        JsonObject setMessage = new JsonObject();
                        setMessage.put("key", cacheKey);
                        setMessage.put("value", histogramData.toString().getBytes());
                        vertx.eventBus().send(
                                RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                setMessage);
                    } catch (PermissionDeniedException
                            | CannotCreateSessionException e) {
                        String v = "Permission denied";
                        log.debug(v);
                        span.error(e);
                        message.fail(403, v);
                    } catch (IllegalArgumentException e) {
                        log.error(
                                "Illegal argument received while retrieving histogram",
                                e);
                        span.error(e);
                        message.fail(400, e.getMessage());
                    } catch (Exception e) {
                        String v = "Exception while retrieving histogram";
                        log.error(v, e);
                        span.error(e);
                        message.fail(500, v);
                    }
                });
    }

    /**
     * Gets the file name and path of the {@link OriginalFile} associated with
     * A given {@link FileAnnotation} and returns them as JSON
     * @param message JSON-encoded event data. Required keys are
     * <code>omeroSessionKey</code> (String) and <code>annotationId</code>
     */
    private void getFileAnnotationMetadata(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        AnnotationCtx annotationCtx;
        try {
            annotationCtx = mapper.readValue(message.body(), AnnotationCtx.class);
        } catch (Exception e) {
            String v = "Illegal annotation context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "get_annotation",
                extractor().extract(annotationCtx.traceContext).context());
        String omeroSessionKey = annotationCtx.omeroSessionKey;
        log.debug("Get annotation request: {}", annotationCtx.toString());
        try (OmeroRequest request = new OmeroRequest(
                host, port, omeroSessionKey)) {
                JsonObject fileInfo = request.execute(
                    new AnnotationRequestHandler(annotationCtx)
                        ::getFileIdAndNameForAnnotation);
                if (fileInfo == null) {
                    message.fail(404, "Cannot find the file path");
                    return;
                }
                fileInfo.put("originalFilePath", ioService.getFilesPath(
                        fileInfo.getLong("originalFileId")));
                message.reply(fileInfo);
        } catch (PermissionDeniedException
                 | CannotCreateSessionException e) {
            String v = "Permission denied";
            message.fail(403, v);
        } catch (Exception e) {
            String v = "Exception while getting annotation";
            log.error(v, e);
            message.fail(500, v);
        } finally {
            span.finish();
        }
    }

    /**
     * Updates the available enumerations from the server.
     * @param client valid client to use to perform actions
     */
    private Void updateFamilies(omero.client client) {
        families = getAllEnumerations(client, Family.class);
        return null;
    }

    /**
     * Updates the available enumerations from the server.
     * @param client valid client to use to perform actions
     */
    private Void updateRenderingModels(omero.client client) {
        renderingModels = getAllEnumerations(client, RenderingModel.class);
        return null;
    }

    /**
     * Retrieves a list of all enumerations from the server of a particular
     * class.
     * @param client valid client to use to perform actions
     * @param klass enumeration class to retrieve.
     * @return See above.
     */
    private <T> List<T> getAllEnumerations(
            omero.client client, Class<T> klass) {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_all_enumerations");
        span.tag("omero.enumeration_class", klass.getName());
        try {
            return client
                    .getSession()
                    .getPixelsService()
                    .getAllEnumerations(klass.getName(), ctx)
                    .stream()
                    .map(x -> {
                        try {
                            return (T) new IceMapper().reverse(x);
                        } catch (ApiUsageException e) {
                            // *Should* never happen
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (ServerError e) {
            span.error(e);
            // *Should* never happen
            throw new RuntimeException(e);
        } finally {
            span.finish();
        }
    }
}
