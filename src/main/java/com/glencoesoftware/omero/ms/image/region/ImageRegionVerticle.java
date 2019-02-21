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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import ome.model.IEnum;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.services.scripts.ScriptFileType;
import ome.system.PreferenceContext;
import ome.api.local.LocalCompress;
import ome.io.nio.PixelsService;
import omeis.providers.re.lut.LutProvider;

public class ImageRegionVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionVerticle.class);

    public static final String GET_ALL_ENUMERATIONS_EVENT =
            "omero.get_all_enumerations";

    public static final String RENDER_IMAGE_REGION_EVENT =
            "omero.render_image_region";

    public static final String RENDER_IMAGE_REGION_PNG_EVENT =
            "omero.render_image_region_png";

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

    /** Available families */
    private List<Family> families;

    /** Available rendering models */
    private List<RenderingModel> renderingModels;

    /** OMERO server pixels service */
    private final PixelsService pixelsService;

    /** Reference to the compression service */
    private final LocalCompress compressionService;

    /**
     * Default constructor.
     */
    public ImageRegionVerticle(ApplicationContext context)
    {
        this.context = context;
        this.preferences =
                (PreferenceContext) this.context.getBean("preferenceContext");
        pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");
        compressionService =
                (LocalCompress) context.getBean("internal-ome.api.ICompress");
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
                    getImageRegion(event);
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
    private void getImageRegion(Message<String> message) {
        StopWatch t0 = new Slf4JStopWatch("getImageRegion");
        ObjectMapper mapper = new ObjectMapper();
        ImageRegionCtx imageRegionCtx;
        try {
            imageRegionCtx = mapper.readValue(
                    message.body(), ImageRegionCtx.class);
        } catch (Exception e) {
            String v = "Illegal image region context";
            log.error(v + ": {}", message.body(), e);
            t0.stop();
            message.fail(400, v);
            return;
        }
        log.debug("Render image region request with data: {}", message.body());

        updateFamilies(imageRegionCtx)
        .thenCompose(this::updateRenderingModels)
        .thenCompose(this::renderImageRegion)
        .whenComplete((imageRegion, t) -> {
            if (t != null) {
                if (t instanceof ReplyException) {
                    // Downstream event handling failure, propagate it
                    t0.stop();
                    message.fail(
                        ((ReplyException) t).failureCode(), t.getMessage());
                } else {
                    String s = "Internal error";
                    log.error(s, t);
                    message.fail(500, s);
                }
            } else if (imageRegion == null) {
                t0.stop();
                message.fail(
                        404, "Cannot find Image:" + imageRegionCtx.imageId);
            } else {
                t0.stop();
                message.reply(imageRegion);
            }
        });
    }

    /**
     * Synchronously performs the action of rendering an image region
     * @param imageRegionCtx {@link ImageRegionCtx} object
     * @return A new CompletionStage that, when this stage completes normally,
     * will provide the rendered image region
     */
    private CompletableFuture<byte[]> renderImageRegion(
            ImageRegionCtx imageRegionCtx) {
        ImageRegionRequestHandler imageRegionRequestHander =
                new ImageRegionRequestHandler(
                        imageRegionCtx, families,
                        renderingModels, lutProvider,
                        pixelsService,
                        compressionService,
                        vertx);
        return imageRegionRequestHander.renderImageRegion();
    }

    /**
     * Updates the available enumerations from the server.
     * @param imageRegionCtx valid image region context used to perform actions
     * @return A new CompletionStage that, when this stage completes normally,
     * will provide the <code>imageRegionCtx</code>
     */
    private CompletableFuture<ImageRegionCtx> updateFamilies(
            ImageRegionCtx imageRegionCtx) {
        if (families == null) {
            return getAllEnumerations(
                    imageRegionCtx, Family.class.getName())
                        .thenApply(enumerations -> {
                            families = (List<Family>) enumerations;
                            return imageRegionCtx;
                        });
        }
        return CompletableFuture.completedFuture(imageRegionCtx);
    }

    /**
     * Updates the available enumerations from the server.
     * @param imageRegionCtx valid image region context used to perform actions
     * @return A new CompletionStage that, when this stage completes normally,
     * will provide the <code>imageRegionCtx</code>
     */
    private CompletableFuture<ImageRegionCtx> updateRenderingModels(
            ImageRegionCtx imageRegionCtx) {
        if (renderingModels == null) {
            return getAllEnumerations(
                    imageRegionCtx, RenderingModel.class.getName())
                        .thenApply(enumerations -> {
                            renderingModels =
                                    (List<RenderingModel>) enumerations;
                            return imageRegionCtx;
                        });
        }
        return CompletableFuture.completedFuture(imageRegionCtx);
    }

    /**
     * Retrieves a list of all enumerations from the server of a particular
     * class.
     * @param client valid client to use to perform actions
     * @param type enumeration class to retrieve.
     * @return See above.
     */
    private CompletableFuture<List<? extends IEnum>> getAllEnumerations(
            ImageRegionCtx imageRegionCtx, String type) {
        CompletableFuture<List<? extends IEnum>> promise =
                new CompletableFuture<>();
        final JsonObject data = new JsonObject();
        data.put("sessionKey", imageRegionCtx.omeroSessionKey);
        data.put("type", type);
        StopWatch t0 = new Slf4JStopWatch(GET_ALL_ENUMERATIONS_EVENT);
        vertx.eventBus().<byte[]>send(
                GET_ALL_ENUMERATIONS_EVENT, data, result -> {
            try {
                if (result.failed()) {
                    t0.stop();
                    promise.completeExceptionally(result.cause());
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                List<? extends IEnum> enumerations =
                        (List<? extends IEnum>) ois.readObject();
                t0.stop();
                promise.complete(enumerations);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                t0.stop();
                promise.completeExceptionally(e);
            }
        });
        return promise;
    }
}
