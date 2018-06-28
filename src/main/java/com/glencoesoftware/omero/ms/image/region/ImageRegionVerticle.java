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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.OmeroRequest;
import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import ome.model.core.Pixels;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.services.scripts.ScriptFileType;
import ome.system.PreferenceContext;
import omeis.providers.re.lut.LutProvider;
import omero.ApiUsageException;
import omero.ServerError;
import omero.util.IceMapper;

public class ImageRegionVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionVerticle.class);

    public static final String RENDER_IMAGE_REGION_EVENT =
            "omero.render_image_region";

    public static final String RENDER_IMAGE_REGION_PNG_EVENT =
            "omero.render_image_region_png";

    /** OMERO server host */
    private final String host;

    /** OMERO server port */
    private final int port;

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /** OMERO server wide preference context. */
    private final PreferenceContext preferences;

    /** Lookup table provider. */
    private final LutProvider lutProvider;

    /** Lookup table OMERO script file type */
    private final ScriptFileType lutType;

    /** Path to the script repository root. */
    private final String scriptRepoRoot;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects.
     */
    private final IceMapper mapper = new IceMapper();

    /** Available families */
    private List<Family> families;

    /** Available rendering models */
    private List<RenderingModel> renderingModels;

    /**
     * Default constructor.
     * @param host OMERO server host.
     * @param port OMERO server port.
     */
    public ImageRegionVerticle(
            String host, int port, ApplicationContext context)
    {
        this.host = host;
        this.port = port;
        this.context = context;
        this.preferences =
                (PreferenceContext) this.context.getBean("preferenceContext");
        scriptRepoRoot = preferences.getProperty("omero.script_repo_root");
        lutType = (ScriptFileType) context.getBean("LUTScripts");
        lutProvider = new LutProviderImpl(new File(scriptRepoRoot), lutType);
    }

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        log.info("Starting verticle");

        vertx.eventBus().<String>consumer(
                RENDER_IMAGE_REGION_EVENT, event -> {
                    renderImageRegion(event);
                });
    }

    /**
     * Creates an OMERO request based on the current context
     * @param imageRegionCtx request context
     * @param message JSON encoded {@link ImageRegionCtx} object.
     * @return See above.
     */
    private OmeroRequest createOmeroRequest(
            ImageRegionCtx imageRegionCtx, Message<String> message) {
        try {
            return new OmeroRequest(
                host, port, imageRegionCtx.omeroSessionKey);
        } catch (PermissionDeniedException
                | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            message.fail(403, v);
        } catch (Exception e) {
            String v = "Exception while creating OMERO request";
            log.error(v, e);
            message.fail(500, v);
        }
        return null;
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
            imageRegionCtx = mapper.readValue(
                    message.body(), ImageRegionCtx.class);
        } catch (Exception e) {
            String v = "Illegal image region context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        log.debug(
            "Render image region request with data: {}", message.body());

        final OmeroRequest request =
                createOmeroRequest(imageRegionCtx, message);
        if (request == null) {
            return;
        }
        Future<Void> future = Future.future();
        future.setHandler(handler -> {
            log.debug("Completing OmeroRequest close future");
            if (request != null) {
                request.close();
            }
        });
        String key = imageRegionCtx.cacheKey();
        vertx.eventBus().<byte[]>send(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
            try {
                if (families == null) {
                    request.execute(this::updateFamilies);
                }
                if (renderingModels == null) {
                    request.execute(this::updateRenderingModels);
                }

                byte[] cachedImageRegion =
                        result.succeeded()? result.result().body() : null;
                ImageRegionRequestHandler requestHandler =
                        new ImageRegionRequestHandler(
                                imageRegionCtx, context, families,
                                renderingModels, lutProvider);
                // If the region is in the cache, check we have permissions
                // to access it and assign and return
                if (cachedImageRegion != null
                        && request.execute(requestHandler::canRead)) {
                    message.reply(cachedImageRegion);
                    future.complete();
                    log.info("Cache HIT {}", key);
                    return;
                }
                log.info("Cache MISS {}", key);

                // The region is not in the cache we have to create it
                String metadataKey = String.format("%s:Image:%d",
                        Pixels.class.getName(), imageRegionCtx.imageId);
                vertx.eventBus().<byte[]>send(
                        RedisCacheVerticle.REDIS_CACHE_GET_EVENT, metadataKey,
                        metadataResult -> {
                    try {
                        Pixels pixels = null;
                        byte[] serialized = null;
                        if (metadataResult.succeeded()) {
                            serialized = metadataResult.result().body();
                        }
                        if (serialized == null) {
                            log.info("Cache MISS {}", metadataKey);
                            pixels = request.execute(
                                    requestHandler::loadPixels);
                            ByteArrayOutputStream bos =
                                    new ByteArrayOutputStream();
                            try (ObjectOutputStream oos =
                                    new ObjectOutputStream(bos)) {
                                oos.writeObject(pixels);
                                serialized = bos.toByteArray();
                            }
                        } else {
                            log.info("Cache HIT {}", metadataKey);
                            try (ObjectInputStream oos = new ObjectInputStream(
                                    new ByteArrayInputStream(serialized))) {
                                pixels = (Pixels) oos.readObject();
                            } catch (Exception e) {
                                log.warn("Failed to deserialize {}", e);
                            }
                        }

                        if (pixels == null) {
                            message.fail(
                                404,
                                "Cannot find Image:" + imageRegionCtx.imageId
                            );
                            future.complete();
                            return;
                        }
                        requestHandler.setPixels(pixels);
                        byte[] imageRegion = request.execute(
                                requestHandler::renderImageRegion);
                        if (imageRegion == null) {
                            message.fail(404, "Cannot render region");
                            future.complete();
                            return;
                        } else {
                            message.reply(imageRegion);
                            future.complete();
                        }

                        // Cache the pixels metadata and image region
                        JsonObject setMessage = new JsonObject();
                        setMessage.put("key", metadataKey);
                        setMessage.put("value", serialized);
                        vertx.eventBus().send(
                                RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                setMessage);
                        setMessage = new JsonObject();
                        setMessage.put("key", key);
                        setMessage.put("value", imageRegion);
                        vertx.eventBus().send(
                                RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                setMessage);
                    } catch (Exception e) {
                        String v = "Exception while retrieving image region";
                        log.error(v, e);
                        message.fail(500, v);
                        future.complete();
                    }
                });
            } catch (IllegalArgumentException e) {
                log.debug(
                    "Illegal argument received while retrieving image " +
                    "region", e);
                message.fail(400, e.getMessage());
                future.complete();
            } catch (Exception e) {
                String v = "Exception while retrieving image region";
                log.error(v, e);
                message.fail(500, v);
                future.complete();
            }
        });
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
        StopWatch t0 = new Slf4JStopWatch("getAllEnumerations");
        try {
            return (List<T>) client
                    .getSession()
                    .getPixelsService()
                    .getAllEnumerations(klass.getName(), ctx)
                    .stream()
                    .map(x -> {
                        try {
                            return (T) mapper.reverse(x);
                        } catch (ApiUsageException e) {
                            // *Should* never happen
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
        } catch (ServerError e) {
            // *Should* never happen
            throw new RuntimeException(e);
        } finally {
            t0.stop();
        }
    }
}
