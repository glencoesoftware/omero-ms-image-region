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

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

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
    private Cache<String, Boolean> canRead;

    /** Whether or not the image region cache is enabled */
    private boolean imageRegionCacheEnabled;

    /** Whether or not the pixels metadata cache is enabled */
    private boolean pixelsMetadataCacheEnabled;

    /** Available families */
    private List<Family> families;

    /** Available rendering models */
    private List<RenderingModel> renderingModels;

    /** Prometheus Summary for createOmeroRequest */
    private static final Summary createOmeroRequestSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("create_omero_request")
      .help("Time to create Omero request")
      .register();

    /** Prometheus Summary for getAllEnumerations*/
    private static final Summary getAllEnumerationsSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("get_all_enumerations")
      .help("Time to get all enumerations")
      .register();

    /** Prometheus Summary for renderImageRegion*/
    private static final Summary renderImageRegionSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("render_image_region_irv")
      .help("Time spent in renderImageRegion in ImageRegionVerticle")
      .register();

    /** Prometheus Summary for getPixels*/
    private static final Summary getPixelsSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("get_pixels_irv")
      .help("Time spent in getPixels in ImageRegionVerticle")
      .register();

    /** Prometheus Summary for loadPixels*/
    private static final Summary loadPixelsSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("load_pixels_irv")
      .help("Time spent in loadPixels in ImageRegionVerticle")
      .register();

    /** Prometheus Summary for getImageRegion*/
    private static final Summary getImageRegionSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("get_image_region")
      .help("Time spent in getImageRegion in ImageRegionVerticle")
      .register();

    /** Prometheus Summary for getCachedImageRegion*/
    private static final Summary getCachedImageRegionSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("get_cached_image_region")
      .help("Time spent in getCachedImageRegion in ImageRegionVerticle")
      .register();

    /** Prometheus Summary for handleRenderImageRegion*/
    private static final Summary handleRenderImageRegionSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("handle_render_image_region")
      .help("Time spent in handleRenderImageRegion in ImageRegionVerticle")
      .register();

    /** Prometheus ImageRegionCache Miss Count*/
    private static final Counter imageRegionCacheMiss = Counter.build()
      .name("image_region_cache_miss")
      .help("Count cache misses in getCachedImageRegion")
      .register();

    /** Prometheus ImageRegionCache Hit Count*/
    private static final Counter imageRegionCacheHit = Counter.build()
      .name("image_region_cache_hit")
      .help("Count cache hits in getCachedImageRegion")
      .register();

    /** Prometheus Pixels Miss Count*/
    private static final Counter pixelsCacheMiss = Counter.build()
      .name("pixels_cache_miss")
      .help("Count cache misses in getPixels")
      .register();

    /** Prometheus Pixels Hit Count*/
    private static final Counter pixelsCacheHit = Counter.build()
      .name("pixels_cache_hit")
      .help("Count cache hits in getPixels")
      .register();

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

        JsonObject canReadCacheConfig =
                config().getJsonObject("can-read-cache");
        long maximumSize = 10000;
        long timeToLive = 0;
        if (canReadCacheConfig != null) {
            maximumSize = canReadCacheConfig.getLong(
                    "maximum-size", maximumSize);
            timeToLive = canReadCacheConfig.getLong(
                    "time-to-live", timeToLive);
        }
        canRead = CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(timeToLive, TimeUnit.SECONDS)
                .build();

        JsonObject imageRegionCacheConfig =
                config().getJsonObject("image-region-cache", new JsonObject());
        imageRegionCacheEnabled =
                imageRegionCacheConfig.getBoolean("enabled", false);
        log.info("Image region cache enabled? {}", imageRegionCacheEnabled);

        JsonObject pixelsMetadataCacheConfig = config()
                .getJsonObject("pixels-metadata-cache", new JsonObject());
        pixelsMetadataCacheEnabled =
                pixelsMetadataCacheConfig.getBoolean("enabled", false);
        log.info("Pixels metadata cache enabled? {}",
                pixelsMetadataCacheEnabled);

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
        Summary.Timer timer = createOmeroRequestSummary.startTimer();
        try {
            return new OmeroRequest(
                host, port, imageRegionCtx.omeroSessionKey);
        } finally {
            timer.observeDuration();
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
        Summary.Timer timer = handleRenderImageRegionSummary.startTimer();
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
        } finally {
          timer.observeDuration();
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
                timer.observeDuration();
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
        Summary.Timer timer = getImageRegionSummary.startTimer();
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
            timer.observeDuration();
            return future;
        }

        Future<byte[]> step1 = getCachedImageRegion(imageRegionCtx, request);

        step1.setHandler(result1 -> {
            if (result1.succeeded()) {
                // If the region is in the cache complete and return
                byte[] imageRegion = result1.result();
                if (imageRegion != null) {
                    timer.observeDuration();
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
                        timer.observeDuration();
                    });
                }
            } else {
                timer.observeDuration();
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
        Summary.Timer timer = renderImageRegionSummary.startTimer();
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
                byte[] imageRegion = requestHandler.renderImageRegion();
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
                timer.observeDuration();
                future.complete(imageRegion);
            } catch (Exception e) {
                timer.observeDuration();
                future.fail(e);
            }
        }, future);

        return future;
    }

    /**
     * Get cached image region if available.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @return Future which will be completed with the image region byte array.
     */
    private Future<byte[]> getCachedImageRegion(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request) {
        Future<byte[]> future = Future.future();

        // Fast exit if the image region cache is disabled
        if (!imageRegionCacheEnabled) {
            future.complete(null);
            return future;
        }

        Summary.Timer timer = getCachedImageRegionSummary.startTimer();
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
                if (imageRegion != null) {
                    Future<Boolean> step1 =
                            canRead(imageRegionCtx, request, requestHandler);

                    step1.compose(canRead -> {
                        if (canRead) {
                            imageRegionCacheHit.inc();
                            timer.observeDuration();
                            future.complete(imageRegion);
                        } else {
                            imageRegionCacheMiss.inc();
                            timer.observeDuration();
                            future.complete(null);
                        }
                    }, future);
                } else {
                    imageRegionCacheMiss.inc();
                    timer.observeDuration();
                    future.complete(null);
                }
            } catch (Exception e) {
                timer.observeDuration();
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
        Summary.Timer timer = getPixelsSummary.startTimer();
        Future<Pixels> future = Future.future();

        String key = String.format("%s:Image:%d",
                Pixels.class.getName(), imageRegionCtx.imageId);
        Future<Pixels> step1 = getCachedPixels(
                imageRegionCtx, request, requestHandler, key);

        step1.setHandler(result1 -> {
            if (result1.succeeded()) {
                // If the pixels metadata is in the cache complete and return
                Pixels pixels = result1.result();
                if (pixels != null) {
                    timer.observeDuration();
                    future.complete(pixels);
                } else {
                    // The pixels metadata  is not in the cache, we have to
                    // load it from the server
                    try {
                        future.complete(
                                loadPixels(request, requestHandler, key));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        timer.observeDuration();
                    }
                }
            } else {
                timer.observeDuration();
                future.fail(result1.cause());
            }
        });

        return future;
    }

    /**
     * Load {@link Pixels} from the server.
     * @param request OMERO request based on the current context
     * @param requestHandler OMERO image region request handler
     * @param key Cache key for {@link Pixels} metadata
     * @return See above.
     */
    private Pixels loadPixels(Supplier<OmeroRequest> request,
            ImageRegionRequestHandler requestHandler, String key)
                    throws ServerError {
        Pixels pixels = request.get().execute(
                requestHandler::loadPixels);
        ByteArrayOutputStream bos =
                new ByteArrayOutputStream();
        Summary.Timer timer = loadPixelsSummary.startTimer();
        try (ObjectOutputStream oos =
                new ObjectOutputStream(bos)) {
            oos.writeObject(pixels);
            byte[] serialized = bos.toByteArray();

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
        } finally {
            timer.observeDuration();
        }
        return pixels;
    }

    /**
     * Get cached {@link Pixels} metadata if available.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @param requestHandler OMERO image region request handler
     * @param key Cache key for {@link Pixels} metadata
     * @return Future which will be completed with the {@link Pixels} metadata.
     */
    private Future<Pixels> getCachedPixels(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request,
            ImageRegionRequestHandler requestHandler, String key) {
        Future<Pixels> future = Future.future();

        // Fast exit if the pixels metadata cache is disabled
        if (!pixelsMetadataCacheEnabled) {
            future.complete(null);
            return future;
        }

        Summary.Timer timer = getPixelsSummary.startTimer();
        vertx.eventBus().<byte[]>send(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
            try {
                final byte[] serialized = result.succeeded()?
                        result.result().body() : null;
                if (serialized != null) {
                    Future<Boolean> step1 =
                            canRead(imageRegionCtx, request, requestHandler);

                    step1.compose(canRead -> {
                        if (canRead) {
                            try (ObjectInputStream oos = new ObjectInputStream(
                                    new ByteArrayInputStream(serialized))) {
                                pixelsCacheHit.inc();
                                future.complete((Pixels) oos.readObject());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                timer.observeDuration();
                            }
                        } else {
                            pixelsCacheMiss.inc();
                            timer.observeDuration();
                            future.complete(null);
                        }
                    }, future);
                } else {
                    pixelsCacheMiss.inc();
                    timer.observeDuration();
                    future.complete(null);
                }
            } catch (Exception e) {
                timer.observeDuration();
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
        Summary.Timer timer = getAllEnumerationsSummary.startTimer();
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
            timer.observeDuration();
        }
    }

    /**
     * Whether or not the current OMERO session can read the metadata required
     * to fulfill the request.
     * @param imageRegionCtx request context
     * @param request OMERO request based on the current context
     * @param requestHandler OMERO image region request handler
     * @return Future which will be completed with the readability.
     */
    private Future<Boolean> canRead(
            ImageRegionCtx imageRegionCtx, Supplier<OmeroRequest> request,
            ImageRegionRequestHandler requestHandler)
                    throws ServerError {
        Future<Boolean> future = Future.future();

        String key = String.format(
                "%s:Image:%d", imageRegionCtx.omeroSessionKey,
                imageRegionCtx.imageId);
        Boolean canRead = this.canRead.getIfPresent(key);
        if (canRead != null) {
            future.complete(canRead);
        } else {
            Future<Boolean> step1 = request.get()
                    .execute(requestHandler::canReadAsync);

            step1.compose(result -> {
                this.canRead.put(key,  result);
                future.complete(result);
            }, future);
        }
        return future;
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
