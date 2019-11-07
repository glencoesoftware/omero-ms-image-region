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

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.OmeroMsAbstractVerticle;
import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;

public class ShapeMaskVerticle extends OmeroMsAbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskVerticle.class);

    public static final String RENDER_SHAPE_MASK_EVENT =
            "omero.render_shape_mask";

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        vertx.eventBus().<String>consumer(
            RENDER_SHAPE_MASK_EVENT,
            event -> {
                getShapeMask(event);
            }
        );
    }

    /**
     * Render shape mask event handler. Responds with a
     * <code>image/png</code> body on success based on the
     * <code>shapeId</code> encoded in the URL or HTTP 404 if the {@link Shape}
     * does not exist or the user does not have permissions to access it.
     * @param message JSON encoded {@link ShapeMaskCtx} object.
     */
    private void getShapeMask(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        ShapeMaskCtx shapeMaskCtx;
        ScopedSpan span;
        try {
            String body = message.body();
            shapeMaskCtx = mapper.readValue(
                    body, ShapeMaskCtx.class);
            span = Tracing.currentTracer().startScopedSpanWithParent(
                    "get_shape_mask",
                    extractor().extract(shapeMaskCtx.traceContext).context());
            span.tag("ctx", body);

        } catch (Exception e) {
            String v = "Illegal shape mask context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        log.debug(
            "Render shape mask request with data: {}", message.body());

        String key = shapeMaskCtx.cacheKey();
        vertx.eventBus().<byte[]>request(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key,
                result -> {
                    byte[] cachedMask =
                            result.succeeded()? result.result().body() : null;
                    ShapeMaskRequestHandler requestHandler =
                            new ShapeMaskRequestHandler(shapeMaskCtx, vertx);

                    requestHandler.canRead()
                    .whenComplete(
                        (readable, throwable) -> {
                            if (throwable != null) {
                                if (throwable instanceof ReplyException) {
                                    // Downstream event handling failure,
                                    // propagate it
                                    span.finish();
                                    message.fail(
                                        ((ReplyException) throwable).failureCode(),
                                        throwable.getMessage());
                                } else {
                                    String s = "Internal error";
                                    log.error(s, throwable);
                                    span.finish();
                                    message.fail(500, s);
                                }
                                return;
                            }

                            if (cachedMask != null && readable) {
                                span.finish();
                                message.reply(cachedMask);
                                return;
                            }

                            requestHandler.renderShapeMask()
                            .whenComplete(
                                (renderedMask, renderThrowable) -> {
                                    if (renderedMask == null) {
                                        if (renderThrowable != null) {
                                            log.error(
                                                "Exception while rendering mask",
                                                renderThrowable);
                                        }
                                        span.finish();
                                        message.fail(404, "Cannot render Mask:" +
                                                shapeMaskCtx.shapeId);
                                        return;
                                    }
                                    span.finish();
                                    message.reply(renderedMask);

                                    // Cache the PNG if the color was explicitly set
                                    if (shapeMaskCtx.color != null) {
                                        JsonObject setMessage = new JsonObject();
                                        setMessage.put("key", key);
                                        setMessage.put("value", renderedMask);
                                        vertx.eventBus().send(
                                            RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                            setMessage);
                                    }
                                }
                            );
                        }
                    );
                }
            );
    }
}
