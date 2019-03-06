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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.Map;
import java.util.Set;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.api.local.LocalCompress;
import ome.io.nio.PixelsService;
import omeis.providers.re.lut.LutProvider;

import com.hazelcast.core.Hazelcast;

public class ImageRegionVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionVerticle.class);

    public static final String GET_ALL_ENUMERATIONS_EVENT =
            "omero.get_all_enumerations";

    public static final String RENDER_IMAGE_REGION_EVENT =
            "omero.render_image_region";

    private static final String CAN_READ_CACHE_NAME =
            "omero.can_read_cache";

    /** Whether or not the image region cache is enabled */
    private boolean imageRegionCacheEnabled;

    /** Whether or not the pixels metadata cache is enabled */
    private boolean pixelsMetadataCacheEnabled;

    /** Lookup table provider. */
    private final LutProvider lutProvider;

    /** Available families */
    private List<Family> families = Arrays.asList(
            new Family(Family.VALUE_EXPONENTIAL),
            new Family(Family.VALUE_LINEAR),
            new Family(Family.VALUE_LOGARITHMIC),
            new Family(Family.VALUE_POLYNOMIAL));

    /** Available rendering models */
    private List<RenderingModel> renderingModels = Arrays.asList(
            new RenderingModel(RenderingModel.VALUE_GREYSCALE),
            new RenderingModel(RenderingModel.VALUE_RGB));

    /** OMERO server pixels service */
    private final PixelsService pixelsService;

    /** Reference to the compression service */
    private final LocalCompress compressionService;

    /** Configured maximum size size in either dimension */
    private final int maxTileLength;

    /** Distributed map for can read caching */
    private Map<String, Boolean> canReadCache;

    /**
     * Default constructor.
     */
    public ImageRegionVerticle(
            PixelsService pixelsService,
            LocalCompress compressionService,
            LutProvider lutProvider,
            int maxTileLength) {
        this.pixelsService = pixelsService;
        this.compressionService = compressionService;
        this.lutProvider = lutProvider;
        this.maxTileLength = maxTileLength;
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        HazelcastInstance hazelcastInstance = instances.stream().findFirst().get();
        canReadCache = hazelcastInstance.getMap(CAN_READ_CACHE_NAME);

    }

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        JsonObject imageRegionCacheConfig =
                config().getJsonObject("image-region-cache", new JsonObject());
        imageRegionCacheEnabled =
                imageRegionCacheConfig.getBoolean("enabled", false);
        JsonObject pixelsMetadataCacheConfig = config()
                .getJsonObject("pixels-metadata-cache", new JsonObject());
        pixelsMetadataCacheEnabled =
                 pixelsMetadataCacheConfig.getBoolean("enabled", false);

        vertx.eventBus().<String>consumer(
            RENDER_IMAGE_REGION_EVENT, new Handler<Message<String>>() {
                @Override
                public void handle(Message<String> event){
                    getImageRegion(event);
                }
            }
        );
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

        renderImageRegion(imageRegionCtx)
        .whenComplete(new BiConsumer<byte[], Throwable>() {
            @Override
            public void accept(byte[] imageRegion, Throwable t) {
                if (t != null) {
                    if (t instanceof ReplyException) {
                        // Downstream event handling failure, propagate it
                        t0.stop();
                        message.fail(
                            ((ReplyException) t).failureCode(), t.getMessage());
                    } else {
                        String s = "Internal error";
                        log.error(s, t);
                        t0.stop();
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
            };
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
                        vertx,
                        maxTileLength,
                        imageRegionCacheEnabled,
                        pixelsMetadataCacheEnabled,
                        canReadCache);
        return imageRegionRequestHander.renderImageRegion();
    }
}
