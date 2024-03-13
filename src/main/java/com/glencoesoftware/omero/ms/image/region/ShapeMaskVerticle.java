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

import com.glencoesoftware.omero.ms.core.PixelsService;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.OmeroMsAbstractVerticle;
import com.glencoesoftware.omero.ms.core.OmeroRequest;
import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.TraceContext;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class ShapeMaskVerticle extends OmeroMsAbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskVerticle.class);

    public static final String RENDER_SHAPE_MASK_EVENT =
            "omero.render_shape_mask";

    public static final String GET_SHAPE_MASK_BYTES_EVENT =
            "omero.get_shape_mask_bytes";

    public static final String GET_LABEL_IMAGE_METADATA_EVENT =
            "omero.get_label_image_metadata";

    /** OMERO server host */
    private String host;

    /** OMERO server port */
    private int port;

    /** Configured Pixels service */
    private final PixelsService pixelsService;

    /**
     * Default constructor.
     */
    public ShapeMaskVerticle(PixelsService pixelsService)
    {
        this.pixelsService = pixelsService;
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
                    RENDER_SHAPE_MASK_EVENT, event -> {
                        renderShapeMask(event);
                    });
            vertx.eventBus().<String>consumer(
                    GET_SHAPE_MASK_BYTES_EVENT, event -> {
                        getShapeMaskBytes(event);
                    });
            vertx.eventBus().<String>consumer(
                    GET_LABEL_IMAGE_METADATA_EVENT, event -> {
                        getLabelImageMetadata(event);
                    });
        } catch (Exception e) {
            startPromise.fail(e);
        }
        startPromise.complete();
    }

    /**
     * Render shape mask event handler. Responds with a
     * <code>image/png</code> body on success based on the
     * <code>shapeId</code> encoded in the URL or HTTP 404 if the {@link Shape}
     * does not exist or the user does not have permissions to access it.
     * @param message JSON encoded {@link ShapeMaskCtx} object.
     */
    private void renderShapeMask(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ShapeMaskCtx shapeMaskCtx;
        ScopedSpan span;
        try {
            String body = message.body();
            shapeMaskCtx = mapper.readValue(body, ShapeMaskCtx.class);
            span = Tracing.currentTracer().startScopedSpanWithParent(
                    "handle_render_shape_mask",
                    extractor().extract(shapeMaskCtx.traceContext).context());
            span.tag("ctx", body);
        } catch (Exception e) {
            String v = "Illegal shape mask context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }

        String key = shapeMaskCtx.cacheKey();
        vertx.eventBus().<byte[]>request(
            RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
                try (OmeroRequest request = new OmeroRequest(
                         host, port, shapeMaskCtx.omeroSessionKey))
                {
                    byte[] shapeMask =
                            result.succeeded()? result.result().body() : null;
                    ShapeMaskRequestHandler requestHandler =
                        new ShapeMaskRequestHandler(
                                shapeMaskCtx, pixelsService);

                    // If the PNG is in the cache, check we have permissions
                    // to access it and assign and return
                    if (shapeMask != null
                            && request.execute(requestHandler::canRead)) {
                        span.finish();
                        message.reply(shapeMask);
                        return;
                    }

                    // The PNG is not in the cache we have to create it
                    shapeMask = request.execute(
                            requestHandler::renderShapeMask);
                    if (shapeMask == null) {
                        span.finish();
                        message.fail(404, "Cannot render Mask:" +
                                shapeMaskCtx.shapeId);
                        return;
                    }
                    span.finish();
                    message.reply(shapeMask);

                    // Cache the PNG if the color was explicitly set
                   if (shapeMaskCtx.color != null) {
                        JsonObject setMessage = new JsonObject();
                        setMessage.put("key", key);
                        setMessage.put("value", shapeMask);
                        vertx.eventBus().send(
                                RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                setMessage);
                    }
                } catch (PermissionDeniedException
                        | CannotCreateSessionException e) {
                    String v = "Permission denied";
                    log.debug(v);
                    span.error(e);
                    message.fail(403, v);
                } catch (IllegalArgumentException e) {
                    log.debug(
                        "Illegal argument received while retrieving shape mask", e);
                    span.error(e);
                    message.fail(400, e.getMessage());
                } catch (Exception e) {
                    String v = "Exception while retrieving shape mask";
                    log.error(v, e);
                    span.error(e);
                    message.fail(500, v);
                }
            }
        );
    }

    /**
     * Get shape mask bytes event handler. Responds with a
     * <code>image/png</code> body on success based on the
     * <code>shapeId</code> encoded in the URL or HTTP 404 if the {@link Shape}
     * does not exist or the user does not have permissions to access it.
     * @param message JSON encoded {@link ShapeMaskCtx} object.
     */
    private void getShapeMaskBytes(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ShapeMaskCtx shapeMaskCtx;
        ScopedSpan span;
        try {
            String body = message.body();
            shapeMaskCtx = mapper.readValue(body, ShapeMaskCtx.class);
            span = Tracing.currentTracer().startScopedSpanWithParent(
                    "handle_get_shape_mask_bytes",
                    extractor().extract(shapeMaskCtx.traceContext).context());
            span.tag("ctx", body);
        } catch (Exception e) {
            String v = "Illegal shape mask context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }

        String key = shapeMaskCtx.cacheKey();
        try (OmeroRequest request = new OmeroRequest(
                 host, port, shapeMaskCtx.omeroSessionKey))
        {
            ShapeMaskRequestHandler requestHandler =
                new ShapeMaskRequestHandler(shapeMaskCtx, pixelsService);

            // The PNG is not in the cache we have to create it
            byte[] shapeMask = request.execute(
                    requestHandler::getShapeMaskBytes);
            if (shapeMask == null) {
                span.finish();
                message.fail(404, "Cannot render Mask:" +
                        shapeMaskCtx.shapeId);
                return;
            }
            span.finish();
            message.reply(shapeMask);

            // Cache the PNG if the color was explicitly set
           if (shapeMaskCtx.color != null) {
                JsonObject setMessage = new JsonObject();
                setMessage.put("key", key);
                setMessage.put("value", shapeMask);
                vertx.eventBus().send(
                        RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                        setMessage);
            }
        } catch (PermissionDeniedException
                | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            span.error(e);
            message.fail(403, v);
        } catch (IllegalArgumentException e) {
            log.debug(
                "Illegal argument received while retrieving shape mask", e);
            span.error(e);
            message.fail(400, e.getMessage());
        } catch (Exception e) {
            String v = "Exception while retrieving shape mask";
            log.error(v, e);
            span.error(e);
            message.fail(500, v);
        }
    }

    private void getLabelImageMetadata(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ShapeMaskCtx shapeMaskCtx;
        ScopedSpan span;
        try {
            String body = message.body();
            shapeMaskCtx = mapper.readValue(body, ShapeMaskCtx.class);
            TraceContext traceCtx = extractor().extract(
                    shapeMaskCtx.traceContext).context();
            span = Tracing.currentTracer().startScopedSpanWithParent(
                    "get_label_image_metadata_verticle",
                    traceCtx);
            span.tag("ctx", body);
        } catch (Exception e) {
            String v = "Illegal shape mask context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        try (OmeroRequest request = new OmeroRequest(
                host, port, shapeMaskCtx.omeroSessionKey))
        {
            JsonObject metadata = null;
            ShapeMaskRequestHandler requestHandler =
                    new ShapeMaskRequestHandler(shapeMaskCtx, pixelsService);
            metadata = request.execute(
                    requestHandler::getLabelImageMetadata);
            if (metadata == null) {
                span.finish();
                message.fail(404, "Cannot get Label Image Metadata:" +
                       shapeMaskCtx.shapeId);
                return;
            }
            span.finish();
            message.reply(metadata);
       } catch (PermissionDeniedException
               | CannotCreateSessionException e) {
           String v = "Permission denied";
           log.debug(v);
           span.error(e);
           message.fail(403, v);
       } catch (IllegalArgumentException e) {
           log.debug(
               "Illegal argument received while retrieving shape mask", e);
           span.error(e);
           message.fail(400, e.getMessage());
       } catch (Exception e) {
           String v = "Exception while retrieving shape mask";
           log.error(v, e);
           span.error(e);
           message.fail(500, v);
       }
    }
}
