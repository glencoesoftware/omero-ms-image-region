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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.python.google.common.base.Throwables;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.OmeroRequest;
import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

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

    /**
     * Cache of read access to certain OMERO objects for a given OMERO session
     */
    private final Cache<String, Boolean> canRead = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

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
                    handleRenderImageRegion(event);
                });
    }

    /**
     * Creates an OMERO request based on the current context
     * @param imageRegionCtx request context
     * @return See above.
     */
    private OmeroRequest createOmeroRequest(ImageRegionCtx imageRegionCtx)
            throws PermissionDeniedException, CannotCreateSessionException,
                ServerError {
        StopWatch t0 = new Slf4JStopWatch("createOmeroRequest");
        try {
            return new OmeroRequest(
                host, port, imageRegionCtx.omeroSessionKey);
        } finally {
            t0.stop();
        }
    }

    /**
     * Render image region event handler. Responds with a
     * request body on success based on the <code>format</code>
     * <code>imageId</code>, <code>z</code> and <code>t</code> encoded in the
     * URL or HTTP 404 if the {@link Image} does not exist or the user
     * does not have permissions to access it.
     * @param message JSON encoded {@link ImageRegionCtx} object.
     */
    private void handleRenderImageRegion(Message<String> message) {
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

        Future<Void> cleanup = Future.future();

        final Supplier<OmeroRequest> request = Suppliers.memoize(() -> {
            OmeroRequest v;
            try {
                v = createOmeroRequest(imageRegionCtx);
                cleanup.setHandler(handler -> {
                    if (v != null) {
                        v.close();
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return v;
        });

        Future<byte[]> step2 = getImageRegion(imageRegionCtx, request);

        step2.setHandler(result -> {
            try {
                if (result.succeeded()) {
                    message.reply(result.result());
                } else {
                    // Unwrap RuntimeException or similar if present
                    Throwable cause = Throwables.getRootCause(result.cause());

                    if (cause instanceof IllegalArgumentException) {
                        message.fail(400, cause.getMessage());
                    } else if (cause instanceof PermissionDeniedException
                        || cause instanceof CannotCreateSessionException) {
                        message.fail(403, "Permission denied");
                    } else if (cause instanceof ObjectNotFoundException) {
                        message.fail(404, cause.getMessage());
                    } else {
                        log.error("Exception retrieving image region", cause);
                        message.fail(500, cause.getMessage());
                    }
                }
            } finally {
                cleanup.complete();
            }
        });
    }

    /**
     * Get image region.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @return Future which will be completed with the image region byte array.
     */
    private Future<byte[]> getImageRegion(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request) {
        Future<byte[]> future = Future.future();

        try {
            if (families == null) {
                request.get().execute(this::updateFamilies);
            }
            if (renderingModels == null) {
                request.get().execute(this::updateRenderingModels);
            }
        } catch (Exception e) {
            future.fail(e);
            return future;
        }

        Future<byte[]> step1 = getCachedImageRegion(imageRegionCtx, request);

        step1.setHandler(result1 -> {
            if (result1.succeeded()) {
                // If the region is in the cache complete and return
                byte[] imageRegion = result1.result();
                if (imageRegion != null) {
                    future.complete(imageRegion);
                } else {
                    // The region is not in the cache, we have to create it
                    Future<byte[]> step2 =
                            renderImageRegion(imageRegionCtx, request);

                    step2.setHandler(result2 -> {
                        if (result2.succeeded()) {
                            future.complete(result2.result());
                        } else {
                            future.fail(result2.cause());
                        }
                    });
                }
            } else {
                future.fail(result1.cause());
            }
        });

        return future;
    }

    /**
     * Render image region.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @return Future which will be completed with the image region byte array.
     */
    private Future<byte[]> renderImageRegion(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request) {
        Future<byte[]> future = Future.future();

        ImageRegionRequestHandler requestHandler =
                new ImageRegionRequestHandler(
                        imageRegionCtx, context, families,
                        renderingModels, lutProvider);
        Future<Pixels> step1 =
                getPixels(imageRegionCtx, request, requestHandler);

        step1.compose(pixels -> {
            try {
                if (pixels == null) {
                    throw new ObjectNotFoundException(
                            "Cannot find Image:" + imageRegionCtx.imageId);
                }
                requestHandler.setPixels(pixels);
                byte[] imageRegion = request.get().execute(
                        requestHandler::renderImageRegion);
                if (imageRegion == null) {
                    throw new ObjectNotFoundException("Cannot render region");
                }
                // Cache the image region
                JsonObject setMessage = new JsonObject();
                setMessage.put("key", imageRegionCtx.cacheKey());
                setMessage.put("value", imageRegion);
                vertx.eventBus().send(
                        RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                        setMessage);
                future.complete(imageRegion);
            } catch (Exception e) {
                future.fail(e);
            }
        }, future);

        return future;
    }

    /**
     * Get cached image region if available or retrieve it from the server.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @return Future which will be completed with the image region byte array.
     */
    private Future<byte[]> getCachedImageRegion(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request) {
        Future<byte[]> future = Future.future();

        String key = imageRegionCtx.cacheKey();
        vertx.eventBus().<byte[]>send(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
            try {
                byte[] imageRegion =
                        result.succeeded()? result.result().body() : null;
                ImageRegionRequestHandler requestHandler =
                        new ImageRegionRequestHandler(
                                imageRegionCtx, context, families,
                                renderingModels, lutProvider);
                if (imageRegion != null
                        && canRead(imageRegionCtx, request, requestHandler)) {
                    log.info("Cache HIT {}", key);
                    future.complete(imageRegion);
                } else {
                    log.info("Cache MISS {}", key);
                    future.complete(null);
                }
            } catch (Exception e) {
                future.fail(e);
            }
        });

        return future;
    }

    /**
     * Get cached {@link Pixels} metadata if available or retrieve it from the
     * server.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @param requestHandler OMERO image region request handler
     * @return Future which will be completed with the {@link Pixels} metadata.
     */
    private Future<Pixels> getPixels(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request,
            ImageRegionRequestHandler requestHandler) {
        Future<Pixels> future = Future.future();

        String key = String.format("%s:Image:%d",
                Pixels.class.getName(), imageRegionCtx.imageId);
        vertx.eventBus().<byte[]>send(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
            try {
                Pixels pixels = null;
                byte[] serialized = null;
                if (result.succeeded()
                        && canRead(imageRegionCtx, request, requestHandler)) {
                    serialized = result.result().body();
                }
                if (serialized == null) {
                    log.info("Cache MISS {}", key);
                    pixels = request.get().execute(
                            requestHandler::loadPixels);
                    ByteArrayOutputStream bos =
                            new ByteArrayOutputStream();
                    try (ObjectOutputStream oos =
                            new ObjectOutputStream(bos)) {
                        oos.writeObject(pixels);
                        serialized = bos.toByteArray();

                        // Cache the pixels metadata and image region
                        JsonObject setMessage = new JsonObject();
                        setMessage.put("key", key);
                        setMessage.put("value", serialized);
                        vertx.eventBus().send(
                                RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                                setMessage);
                    } catch (IOException e) {
                        log.error("IO error serializing Pixels:{}",
                                pixels.getId(), e);
                    }
                } else {
                    log.info("Cache HIT {}", key);
                    try (ObjectInputStream oos = new ObjectInputStream(
                            new ByteArrayInputStream(serialized))) {
                        pixels = (Pixels) oos.readObject();
                    } catch (Exception e) {
                        log.error("Failed to deserialize", e);
                    }
                }
                future.complete(pixels);
            } catch (Exception e) {
                future.fail(e);
            }
        });

        return future;
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

    /**
     * Whether or not the current OMERO session can read the metadata required
     * to fulfill the request.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @param requestHandler OMERO image region request handler
     * @return See above.
     */
    private Boolean canRead(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request,
            ImageRegionRequestHandler requestHandler)
                    throws ServerError, ExecutionException {
        String key = String.format(
                "%s:Image:%d", imageRegionCtx.omeroSessionKey,
                imageRegionCtx.imageId);
        return canRead.get(key, () -> {
            return request.get().execute(requestHandler::canRead);
        });
    }

    /**
     * Thrown whenever an object cannot be found.
     */
    private class ObjectNotFoundException extends Exception {

        ObjectNotFoundException(String message) {
            super(message);
        }

    }
}
