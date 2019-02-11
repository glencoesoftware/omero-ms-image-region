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
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.Json;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.services.scripts.ScriptFileType;
import ome.system.PreferenceContext;
import ome.api.local.LocalCompress;
import ome.io.nio.PixelsService;
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
        log.info(
            "Render image region request with data: {}", message.body());
        log.info("Session Key: {}", imageRegionCtx.omeroSessionKey);

        if (families == null) {
            try {
                families = getFamilies(imageRegionCtx.omeroSessionKey).get();//TODO; This is blocking
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        String sessionKey = imageRegionCtx.omeroSessionKey;
        updateFamilies(sessionKey)
        .thenCompose((myVoid) -> {return updateRenderingModels(sessionKey);})
        .thenCompose((myOtherVoid) -> {
            PixelsService pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");
            LocalCompress compressionService =
                    (LocalCompress) context.getBean("internal-ome.api.ICompress");
            ImageRegionRequestHandler imageRegionRequestHander =
                    new ImageRegionRequestHandler(
                            imageRegionCtx, context, families,
                            renderingModels, lutProvider,
                            pixelsService,                            
                            compressionService,
                            vertx);
            CompletableFuture<byte[]> imageRegionFuture =
                    imageRegionRequestHander.renderImageRegionAsync(sessionKey);
            return imageRegionFuture;
            })
        .thenAccept((imageRegion) -> {
            if (imageRegion == null) {
                message.fail(
                        404, "Cannot find Image:" + imageRegionCtx.imageId);
            } else {
                message.reply(imageRegion);
            }
        });
        /*
        } catch (PermissionDeniedException
                | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            message.fail(403, v);
        } catch (IllegalArgumentException e) {
            log.debug(
                "Illegal argument received while retrieving image region", e);
            message.fail(400, e.getMessage());
        } catch (Exception e) {
            String v = "Exception while retrieving image region";
            log.error(v, e);
            message.fail(500, v);
        }
        */
    }

    /**
     * Updates the available enumerations from the server.
     * @param client valid client to use to perform actions
     */
    private CompletableFuture<Void> updateFamilies(String sessionKey) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        final Map<String, Object> data = new HashMap<String, Object>();
        data.put("sessionKey", sessionKey);
        data.put("type", Family.class.getName());
        vertx.eventBus().<byte[]>send(
                ImageRegionRequestHandler.GET_ALL_ENUMERATIONS_EVENT,
                Json.encode(data), result -> {
            String s = "";
            try {
                if (result.failed()) {
                    Throwable t = result.cause();
                    promise.completeExceptionally(t);
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                families = (List<Family>) ois.readObject();
                promise.complete(null);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);

            }
        });
        return promise;
    }
    
    /**
     * Updates the available enumerations from the server.
     * @param client valid client to use to perform actions
     */
    private CompletableFuture<Void> updateRenderingModels(String sessionKey) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        final Map<String, Object> data = new HashMap<String, Object>();
        data.put("sessionKey", sessionKey);
        data.put("type", RenderingModel.class.getName());
        vertx.eventBus().<byte[]>send(
                ImageRegionRequestHandler.GET_ALL_ENUMERATIONS_EVENT,
                Json.encode(data), result -> {
            String s = "";
            try {
                if (result.failed()) {
                    Throwable t = result.cause();
                    promise.completeExceptionally(t);
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                renderingModels = (List<RenderingModel>) ois.readObject();
                promise.complete(null);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);

            }
        });
        return promise;
    }

    /**
     * Updates the available enumerations from the server.
     * @param client valid client to use to perform actions
     */
    private CompletableFuture<List<Family>> getFamilies(String sessionKey) {
        CompletableFuture<List<Family>> promise = new CompletableFuture<List<Family>>();
        final Map<String, Object> data = new HashMap<String, Object>();
        data.put("sessionKey", sessionKey);
        data.put("type", Family.class.getName());
        vertx.eventBus().<byte[]>send(
                ImageRegionRequestHandler.GET_ALL_ENUMERATIONS_EVENT,
                Json.encode(data), result -> {
            String s = "";
            try {
                if (result.failed()) {
                    Throwable t = result.cause();
                    promise.completeExceptionally(t);
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                List<Family> families = (List<Family>) ois.readObject();
                promise.complete(families);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);

            }
        });
        return promise;
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
