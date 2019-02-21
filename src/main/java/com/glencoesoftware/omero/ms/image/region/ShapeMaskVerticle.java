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

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class ShapeMaskVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ShapeMaskVerticle.class);

    public static final String RENDER_SHAPE_MASK_EVENT =
            "omero.render_shape_mask";

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        log.info("Starting verticle");

        vertx.eventBus().<String>consumer(
                RENDER_SHAPE_MASK_EVENT, event -> {
                    getShapeMask(event);
                });
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
        try {
            shapeMaskCtx = mapper.readValue(
                    message.body(), ShapeMaskCtx.class);
        } catch (Exception e) {
            String v = "Illegal shape mask context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        log.debug(
            "Render shape mask request with data: {}", message.body());

        String key = shapeMaskCtx.cacheKey();
        StopWatch t0 = new Slf4JStopWatch("getShapeMask");
        vertx.eventBus().<byte[]>send(
            RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
                byte[] cachedMask =
                        result.succeeded()? result.result().body() : null;
                ShapeMaskRequestHandler requestHandler =
                        new ShapeMaskRequestHandler(shapeMaskCtx, vertx);

                requestHandler.canRead()
                .thenAccept(readable -> {
                    if (cachedMask != null && readable) {
                        t0.stop();
                        message.reply(cachedMask);
                        return;
                    }

                    requestHandler.renderShapeMask()
                    .whenComplete((rendereredMask, t) -> {
                        if (rendereredMask == null) {
                            if (t != null) {
                                log.error("Exception while rendering mask", t);
                            }
                            t0.stop();
                            message.fail(404, "Cannot render Mask:" +
                                    shapeMaskCtx.shapeId);
                            return;
                        }
                        t0.stop();
                        message.reply(rendereredMask);

                        // Cache the PNG if the color was explicitly set
                        if (shapeMaskCtx.color != null) {
                            JsonObject setMessage = new JsonObject();
                            setMessage.put("key", key);
                            setMessage.put("value", rendereredMask);
                            vertx.eventBus().send(
                                    RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                    setMessage);
                        }
                    });
                });
            }
        );
    }
}
