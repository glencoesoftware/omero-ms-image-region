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

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.lang.IllegalArgumentException;
import java.lang.Math;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ServiceRegistry;
import javax.imageio.stream.ImageOutputStream;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.RedisCacheVerticle;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriter;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriterSpi;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonObject;
import io.vertx.core.Vertx;
import ome.api.local.LocalCompress;
import ome.io.nio.InMemoryPlanarPixelBuffer;
import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.QuantumDef;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.util.ImageUtil;
import omeis.providers.re.Renderer;
import omeis.providers.re.RenderingStats;
import omeis.providers.re.codomain.ReverseIntensityContext;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.metadata.StatsFactory;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumFactory;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    public static final String GET_PIXELS_DESCRIPTION_EVENT =
            "omero.get_pixels_description";

    public static final String CAN_READ_EVENT =
            "omero.can_read";

    /** OMERO server pixels service */
    private final PixelsService pixelsService;

    /** Reference to the compression service */
    private final LocalCompress compressionService;

    /** Reference to the projection service */
    private final ProjectionService projectionService;

    /** Lookup table provider */
    private final LutProvider lutProvider;

    /** Image Region Context */
    private final ImageRegionCtx imageRegionCtx;

    /** Renderer */
    private Renderer renderer;

    /** Available families */
    private final List<Family> families;

    /** Available rendering models */
    private final List<RenderingModel> renderingModels;

    /** Configured maximum size size in either dimension */
    private final int maxTileLength;

    /** Whether or not the image region cache is enabled */
    private final boolean imageRegionCacheEnabled;

    /** Whether or not the pixels metadata cache is enabled */
    private final boolean pixelsMetadataCacheEnabled;

    private Map<String, Boolean> canReadCache;

    /** Handle on Vertx for event bus work*/
    private Vertx vertx;

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            PixelsService pixelsService,
            LocalCompress compressionService,
            Vertx vertx,
            int maxTileLength,
            boolean imageRegionCacheEnabled,
            boolean pixelsMetadataCacheEnabled,
            Map<String, Boolean> canReadCache) {
        log.info("Setting up handler");
        this.imageRegionCtx = imageRegionCtx;
        this.families = families;
        this.renderingModels = renderingModels;
        this.lutProvider = lutProvider;
        this.pixelsService = pixelsService;
        this.compressionService = compressionService;
        this.vertx = vertx;
        this.maxTileLength = maxTileLength;
        this.imageRegionCacheEnabled = imageRegionCacheEnabled;
        this.pixelsMetadataCacheEnabled = pixelsMetadataCacheEnabled;
        this.canReadCache = canReadCache;

        projectionService = new ProjectionService();
    }

    /**
     * Render Image region request handler.
     */
    public CompletableFuture<byte[]> renderImageRegion() {
        return getCachedImageRegion().thenCompose(result -> {
            if (result != null) {
                log.debug("Image region cache hit");
                return CompletableFuture.completedFuture(result);
            }
            log.debug("Image region cache miss");

            return getPixelsDescription()
                    .thenApply(this::createRenderingDef)
                    .thenApply(this::getRegion);
        });
    }

    /**
     * Whether or not the current OMERO session can read the metadata required
     * to fulfill the request.
     * @return Future which will be completed with the readability.
     */
    private CompletableFuture<Boolean> canRead() {
        final JsonObject data = new JsonObject();
        data.put("sessionKey", imageRegionCtx.omeroSessionKey);
        data.put("type", "Image");
        data.put("id", imageRegionCtx.imageId);
        Boolean canRead = canReadCache.get(imageRegionCtx.cacheKey);
        if (canRead != null) {
            log.debug("Can read {} cache hit", imageRegionCtx.cacheKey);
            return CompletableFuture.completedFuture(canRead);
        } else {
            log.debug("Can read {} cache miss", imageRegionCtx.cacheKey);
            ScopedSpan span = Tracing.currentTracer().startScopedSpan("can_read");
            CompletableFuture<Boolean> promise = new CompletableFuture<>();
            vertx.eventBus().<Boolean>request(
                CAN_READ_EVENT, data, result -> {
                    if (result.failed()) {
                        span.finish();
                        promise.completeExceptionally(result.cause());
                        return;
                    }
                    span.finish();
                    //Put the result in the cache
                    Boolean v = result.result().body();
                    canReadCache.put(imageRegionCtx.cacheKey, v);
                    promise.complete(v);
                }
            );

            return promise;
        }
    }

    /**
     * Get cached image region if available.
     * @return Future which will be completed with the image region byte array.
     */
    private CompletableFuture<byte[]> getCachedImageRegion() {
        // Fast exit if the image region cache is disabled
        if (!imageRegionCacheEnabled) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        String key = imageRegionCtx.cacheKey();
        vertx.eventBus().<byte[]>request(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
            if (result.failed()) {
                future.completeExceptionally(result.cause());
                return;
            }

             byte[] body =
                     result.succeeded()? result.result().body() : null;
             if (body != null) {
                 canRead().whenComplete((canRead, t) -> {
                     if (t != null) {
                         future.completeExceptionally(t);
                         return;
                     }

                     if (canRead) {
                         future.complete(body);
                     } else {
                         future.complete(null);
                     }
                 });
             } else {
                 future.complete(null);
             }
         });
         return future;
    }

    /**
     * Creates a new set of rendering settings corresponding to the specified
     * pixels set.  Most values will be overwritten during
     * {@link #updateSettings(Renderer)} usage.
     * @param pixels pixels metadata
     * @return See above.
     */
    private RenderingDef createRenderingDef(Pixels pixels) {
        QuantumFactory quantumFactory = new QuantumFactory(families);
        StatsFactory statsFactory = new StatsFactory();

        RenderingDef renderingDef = new RenderingDef();
        renderingDef.setPixels(pixels);
        // Model will be reset during updateSettings()
        renderingModels.forEach(model -> {
            if (model.getValue().equals(Renderer.MODEL_GREYSCALE)) {
                renderingDef.setModel(model);
            }
        });
        // QuantumDef defaults cribbed from
        // ome.logic.RenderingSettingsImpl#resetDefaults().  These *will not*
        // be reset by updateSettings().
        QuantumDef quantumDef = new QuantumDef();
        quantumDef.setCdStart(0);
        quantumDef.setCdEnd(QuantumFactory.DEPTH_8BIT);
        quantumDef.setBitResolution(QuantumFactory.DEPTH_8BIT);
        renderingDef.setQuantization(quantumDef);
        // ChannelBinding defaults cribbed from
        // ome.logic.RenderingSettingsImpl#resetChannelBindings().  All *will*
        // be reset by updateSettings() unless otherwise denoted.
        for (int c = 0; c < pixels.sizeOfChannels(); c++) {
            double[] range = statsFactory.initPixelsRange(pixels);
            ChannelBinding cb = new ChannelBinding();
            // Will *not* be reset by updateSettings()
            cb.setCoefficient(1.0);
            // Will *not* be reset by updateSettings()
            cb.setNoiseReduction(false);
            // Will *not* be reset by updateSettings()
            cb.setFamily(quantumFactory.getFamily(Family.VALUE_LINEAR));
            cb.setInputStart(range[0]);
            cb.setInputEnd(range[1]);
            cb.setActive(c < 3);
            cb.setRed(255);
            cb.setBlue(0);
            cb.setGreen(0);
            cb.setAlpha(255);
            renderingDef.addChannelBinding(cb);
        }
        return renderingDef;
    }

    private PixelBuffer getPixelBuffer(Pixels pixels) {
        ScopedSpan span = Tracing.currentTracer().startScopedSpan("PixelsService.get_pixel_buffer");
        try {
            return pixelsService.getPixelBuffer(pixels,false);
        } finally {
            span.finish();
        }
    }

    /**
     * Get cached {@link Pixels} metadata if available or retrieve it from the
     * server.
     * @return Future which will be completed with the {@link Pixels} metadata.
     */
    private CompletableFuture<Pixels> getPixelsDescription() {
        String key = String.format("%s:Image:%d",
                Pixels.class.getName(), imageRegionCtx.imageId);
        return getCachedPixelsDescription(key).thenCompose(result -> {
            if (result != null) {
                log.debug("Pixels description cache hit");
                return CompletableFuture.completedFuture(result);
            }
            log.debug("Pixels description cache miss");

            return loadPixelsDescription(key);
        });
    }

    /**
     * Load {@link Pixels} from the server.
     * @param request OMERO request based on the current context
     * @param requestHandler OMERO image region request handler
     * @param key Cache key for {@link Pixels} metadata
     * @return See above.
     */
    private CompletableFuture<Pixels> loadPixelsDescription(String key) {
        CompletableFuture<Pixels> promise = new CompletableFuture<>();

        final JsonObject data = new JsonObject();
        data.put("sessionKey", imageRegionCtx.omeroSessionKey);
        data.put("imageId", imageRegionCtx.imageId);
        ScopedSpan span = Tracing.currentTracer().startScopedSpan(GET_PIXELS_DESCRIPTION_EVENT);
        vertx.eventBus().<byte[]>request(
                GET_PIXELS_DESCRIPTION_EVENT, data, result -> {
            try {
                if (result.failed()) {
                    span.finish();
                    promise.completeExceptionally(result.cause());
                    return;
                }

                byte[] body = result.result().body();
                ByteArrayInputStream bais = new ByteArrayInputStream(body);
                ObjectInputStream ois = new ObjectInputStream(bais);
                Pixels pixels = (Pixels) ois.readObject();
                span.finish();
                promise.complete(pixels);

                // Cache the pixels metadata
                if (pixelsMetadataCacheEnabled) {
                    JsonObject setMessage = new JsonObject();
                    setMessage.put("key", key);
                    setMessage.put("value", body);
                    vertx.eventBus().send(
                            RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                            setMessage);
                }
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                span.finish();
                promise.completeExceptionally(e);
            }
        });

        return promise;
    }

    /**
     * Get cached {@link Pixels} metadata if available.
     * @param key Cache key for {@link Pixels} metadata
     * @return Future which will be completed with the {@link Pixels} metadata.
     */
    private CompletableFuture<Pixels> getCachedPixelsDescription(String key) {
        // Fast exit if the pixels metadata cache is disabled
        if (!pixelsMetadataCacheEnabled) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Pixels> future = new CompletableFuture<>();
        vertx.eventBus().<byte[]>request(
                RedisCacheVerticle.REDIS_CACHE_GET_EVENT, key, result -> {
            if (result.failed()) {
                future.completeExceptionally(result.cause());
                return;
            }

            byte[] data = result.result().body();
            if (data == null) {
                future.complete(null);
                return;
            }
            canRead().whenComplete((canRead, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                    return;
                }

                if (canRead) {
                    try {
                        ByteArrayInputStream bais =
                                new ByteArrayInputStream(data);
                        ObjectInputStream ois = new ObjectInputStream(bais);
                        Pixels pixels = (Pixels) ois.readObject();
                        future.complete(pixels);
                    } catch (IOException | ClassNotFoundException e) {
                        log.error(
                            "Exception while decoding object in response", e);
                        future.completeExceptionally(e);
                    }
                } else {
                    future.complete(null);
                }
            });
        });
        return future;
    }

    private byte[] getRegion(RenderingDef renderingDef) {
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            QuantumFactory quantumFactory = new QuantumFactory(families);
            Pixels pixels = renderingDef.getPixels();
            PixelBuffer pixelBuffer = getPixelBuffer(pixels);
            renderer = new Renderer(
                quantumFactory, renderingModels,
                pixels, renderingDef,
                pixelBuffer, lutProvider
            );
            PlaneDef planeDef = new PlaneDef(PlaneDef.XY, imageRegionCtx.t);
            planeDef.setZ(imageRegionCtx.z);

            // Avoid asking for resolution descriptions if there is no image
            // pyramid.  This can be *very* expensive.
            int countResolutionLevels = pixelBuffer.getResolutionLevels();
            List<List<Integer>> resolutionLevels;
            if (countResolutionLevels > 1) {
                resolutionLevels = pixelBuffer.getResolutionDescriptions();
            } else {
                resolutionLevels = new ArrayList<List<Integer>>();
                resolutionLevels.add(
                        Arrays.asList(pixels.getSizeX(), pixels.getSizeY()));
            }
            planeDef.setRegion(getRegionDef(resolutionLevels, pixelBuffer));
            setResolutionLevel(renderer, resolutionLevels);
            if (imageRegionCtx.compressionQuality != null) {
                compressionService.setCompressionLevel(
                        imageRegionCtx.compressionQuality);
            }
            updateSettings(renderer);
            // The actual act of rendering will close the provided pixel
            // buffer.  However, just in case an exception is thrown before
            // reaching this point a double close may occur due to the
            // surrounding try-with-resources block.
            byte[] region = render(
                    renderer, resolutionLevels, pixels, planeDef,
                    pixelBuffer);
            // Cache the image region
            if (imageRegionCacheEnabled) {
                JsonObject setMessage = new JsonObject();
                setMessage.put("key", imageRegionCtx.cacheKey);
                setMessage.put("value", region);
                vertx.eventBus().send(
                    RedisCacheVerticle.REDIS_CACHE_SET_EVENT,
                    setMessage);
            }
            return region;
        } catch (IOException | QuantizationException e) {
            throw new CompletionException(e);
        }
    }

    /**
     * Performs conditional rendering in the requested format as defined by
     * <code>imageRegionCtx.format</code>.
     * @param renderer fully initialized renderer
     * @param resolutionLevels complete definition of all resolution levels
     * for the image.
     * @param pixels pixels metadata
     * @param planeDef plane definition to use for rendering
     * @return Image region as a byte array.
     * @throws IOException
     * @throws QuantizationException
     */
    private byte[] render(
            Renderer renderer, List<List<Integer>> resolutionLevels,
            Pixels pixels, PlaneDef planeDef, PixelBuffer pixelBuffer)
                    throws IOException, QuantizationException {
        checkPlaneDef(resolutionLevels, planeDef);

        ScopedSpan span = Tracing.currentTracer().startScopedSpan("Renderer.render_as_packed_int");
        int[] buf;
        try {
            PixelBuffer newBuffer = null;
            if (imageRegionCtx.projection != null) {
                byte[][][][] planes = new byte[1][pixels.getSizeC()][1][];
                int projectedSizeC = 0;
                ChannelBinding[] channelBindings =
                        renderer.getChannelBindings();
                int start = Optional
                        .ofNullable(imageRegionCtx.projectionStart)
                        .orElse(0);
                int end = Optional
                        .ofNullable(imageRegionCtx.projectionEnd)
                        .orElse(pixels.getSizeZ() - 1);
                try {
                    for (int i = 0; i < channelBindings.length; i++) {
                        if (!channelBindings[i].getActive()) {
                            continue;
                        }
                        ScopedSpan span2 = Tracing.currentTracer().startScopedSpan("ProjectionService.project_stack");
                        try {
                            planes[0][i][0] = projectionService.projectStack(
                                pixels,
                                pixelBuffer,
                                imageRegionCtx.projection,
                                imageRegionCtx.t,
                                i,  // Channel index
                                1,  // Stepping 1 in ImageWrapper.renderJpeg()
                                start,
                                end
                            );
                        } finally {
                            span.finish();
                        }
                        projectedSizeC++;
                    }
                } finally {
                    pixelBuffer.close();
                }
                Pixels projectedPixels = new Pixels(
                    pixels.getImage(),
                    pixels.getPixelsType(),
                    pixels.getSizeX(),
                    pixels.getSizeY(),
                    1,  // Z
                    projectedSizeC,
                    1,  // T
                    "",
                    pixels.getDimensionOrder()
                );
                newBuffer = new InMemoryPlanarPixelBuffer(
                        projectedPixels, planes);
                planeDef = new PlaneDef(PlaneDef.XY, 0);
                planeDef.setZ(0);
            }
            buf =  renderer.renderAsPackedInt(planeDef, newBuffer);
        } finally {
            span.finish();
            if (log.isDebugEnabled()) {
                RenderingStats stats = renderer.getStats();
                if (stats != null) {
                    log.debug(renderer.getStats().getStats());
                }
            }
        }

        String format = imageRegionCtx.format;
        RegionDef region = planeDef.getRegion();
        int sizeX = region != null? region.getWidth() : pixels.getSizeX();
        int sizeY = region != null? region.getHeight() : pixels.getSizeY();
        buf = flip(buf, sizeX, sizeY,
                imageRegionCtx.flipHorizontal, imageRegionCtx.flipVertical);
        BufferedImage image = ImageUtil.createBufferedImage(
            buf, sizeX, sizeY
        );
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        if (format.equals("jpeg")) {
            compressionService.compressToStream(image, output);
            return output.toByteArray();
        } else if (format.equals("png") || format.equals("tif")) {
            if (format.equals("tif")) {
                try (ImageOutputStream ios =
                        ImageIO.createImageOutputStream(output)) {
                    IIORegistry registry = IIORegistry.getDefaultInstance();
                    registry.registerServiceProviders(
                            ServiceRegistry.lookupProviders(
                                    TIFFImageWriterSpi.class));
                    TIFFImageWriterSpi spi = registry.getServiceProviderByClass(
                            TIFFImageWriterSpi.class);
                    TIFFImageWriter writer = new TIFFImageWriter(spi);
                    writer.setOutput(ios);
                    writer.write(null, new IIOImage(image, null, null), null);
                }
            } else {
                ImageIO.write(image, "png", output);
            }
            return output.toByteArray();
        }
        log.error("Unknown format {}", imageRegionCtx.format);
        return null;
    }

    /**
     * Flip an image horizontally, vertically, or both.
     * @param src source image buffer
     * @param sizeX size of <code>src</code> in X (number of columns)
     * @param sizeY size of <code>src</code> in Y (number of rows)
     * @param flipHorizontal whether or not to flip the image horizontally
     * @param flipVertical whether or not to flip the image vertically
     * @return Newly allocated buffer with flipping applied or <code>src</code>
     * if no flipping has been requested.
     */
    public static int[] flip(
            int[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        if (!flipHorizontal && !flipVertical) {
            return src;
        }

        if (src == null) {
            throw new IllegalArgumentException("Attempted to flip null image");
        } else if (sizeX == 0 || sizeY == 0) {
            throw new IllegalArgumentException("Attempted to flip image with 0 size");
        }

        int[] dest = new int[src.length];
        int srcIndex, destIndex;
        int xOffset = flipHorizontal? sizeX : 1;
        int yOffset = flipVertical? sizeY : 1;
        for (int x = 0; x < sizeX; x++) {
            for (int y = 0; y < sizeY; y++) {
                srcIndex = (y * sizeX) + x;
                destIndex = Math.abs(((yOffset - y - 1) * sizeX))
                        + Math.abs((xOffset - x - 1));
                dest[destIndex] = src[srcIndex];
            }
        }
        return dest;
    }

    /**
     * Copied from {@link RenderingBean#checkPlaneDef}. A client side version
     * of this is required when we are rendering uncompressed.
     * @param resolutionLevels complete definition of all resolution levels
     * for the image.
     * @param planeDef plane definition to validate
     */
    private void checkPlaneDef(
            List<List<Integer>> resolutionLevels, PlaneDef planeDef) {
        RegionDef rd = planeDef.getRegion();
        if (rd == null) {
            return;
        }
        int resolution =
                Optional.ofNullable(imageRegionCtx.resolution).orElse(0);
        int sizeX = resolutionLevels.get(resolution).get(0);
        int sizeY = resolutionLevels.get(resolution).get(1);
        if (rd.getWidth() + rd.getX() > sizeX) {
            int newWidth = sizeX - rd.getX();
            log.debug("Resetting out of bounds region XOffset {} width {}" +
                    " vs. sizeX {} to {}",
                    rd.getX(), rd.getWidth(), sizeX, newWidth);
            rd.setWidth(newWidth);
        } else {
            log.debug("Leaving region xOffset {} width {} alone vs. sizeX {}",
                    rd.getX(), rd.getWidth(), sizeX);
        }
        if (rd.getHeight() + rd.getY() > sizeY) {
            int newHeight = sizeY - rd.getY();
            log.debug("Resetting out of bounds region yOffset {} height {}" +
                    " vs. sizeY {} to {}",
                    rd.getY(), rd.getHeight(), sizeY, newHeight);
            rd.setHeight(newHeight);
        } else {
            log.debug("Leaving region yOffset {} height {} alone vs. " +
                    "sizeY {}", rd.getY(), rd.getHeight(), sizeY);
        }
    }

    /**
     * Update settings on the rendering engine based on the current context.
     * @param renderer fully initialized renderer
     * @param sizeC number of channels
     * @param ctx OMERO context (group)
     */
    private void updateSettings(Renderer renderer) {
        log.debug("Setting active channels");
        int idx = 0; // index of windows/colors args
        for (int c = 0; c < renderer.getMetadata().getSizeC(); c++) {
            log.debug("Setting for channel {}", c);
            boolean isActive = imageRegionCtx.channels.contains(c + 1);
            log.debug("\tChannel active {}", isActive);
            renderer.setActive(c, isActive);

            if (isActive) {
                if (imageRegionCtx.windows != null) {
                    double min = imageRegionCtx.windows.get(idx)[0];
                    double max = imageRegionCtx.windows.get(idx)[1];
                    log.debug("\tMin-Max: [{}, {}]", min, max);
                    renderer.setChannelWindow(c, min, max);
                }
                if (imageRegionCtx.colors != null) {
                    String color = imageRegionCtx.colors.get(idx);
                    if (color.endsWith(".lut")) {
                        renderer.setChannelLookupTable(c, color);
                        log.debug("\tLUT: {}", color);
                    } else {
                        int[] rgba = splitHTMLColor(color);
                        renderer.setRGBA(c, rgba[0], rgba[1],rgba[2], rgba[3]);
                        log.debug("\tColor: [{}, {}, {}, {}]",
                                  rgba[0], rgba[1], rgba[2], rgba[3]);
                    }
                }
                if (imageRegionCtx.maps != null) {
                    if (c < imageRegionCtx.maps.size()) {
                        Map<String, Map<String, Object>> map =
                                imageRegionCtx.maps.get(c);
                        if (map != null) {
                            Map<String, Object> reverse = map.get("reverse");
                            if (reverse != null
                                && Boolean.TRUE.equals(reverse.get("enabled"))) {
                                renderer.getCodomainChain(c).add(
                                        new ReverseIntensityContext());
                            }
                        }
                    }
                }
            }

            idx += 1;
        }
        for (RenderingModel renderingModel : renderingModels) {
            if (imageRegionCtx.m.equals(renderingModel.getValue())) {
                renderer.setModel(renderingModel);
                break;
            }
        }
    }

    /**
     * Update RegionDef to fit within the image boundaries.
     * @param sizeX width of the image at the current resolution
     * @param sizeY height of the image at the current resolution
     * @param regionDef region definition to truncate if required
     * @throws IllegalArgumentException
     * @see ImageRegionRequestHandler#getRegionDef(Pixels, PixelBuffer)
     */
    protected void truncateRegionDef(
            int sizeX, int sizeY, RegionDef regionDef) {
        log.debug("Truncating RegionDef if required");
        regionDef.setWidth(Math.min(
                regionDef.getWidth(), sizeX - regionDef.getX()));
        regionDef.setHeight(Math.min(
                regionDef.getHeight(), sizeY - regionDef.getY()));
    }

    /**
     * Update RegionDef to be flipped if required.
     * @param sizeX width of the image at the current resolution
     * @param sizeY height of the image at the current resolution
     * @param tileSize XY tile sizes of the underlying pixels
     * @param regionDef region definition to flip if required
     * @throws IllegalArgumentException
     * @throws ServerError
     * @see ImageRegionRequestHandler#getRegionDef(Pixels, PixelBuffer)
     */
    protected void flipRegionDef(int sizeX, int sizeY, RegionDef regionDef) {
        log.debug("Flipping tile RegionDef if required");
        if (imageRegionCtx.flipHorizontal) {
            regionDef.setX(
                    sizeX - regionDef.getWidth() - regionDef.getX());
        }
        if (imageRegionCtx.flipVertical) {
            regionDef.setY(
                    sizeY - regionDef.getHeight() - regionDef.getY());
        }
    }

    /**
     * Returns RegionDef to read based on tile / region provided in
     * ImageRegionCtx.
     * @param resolutionLevels complete definition of all resolution levels
     * @param pixelBuffer raw pixel data access buffer
     * @return RegionDef {@link RegionDef} describing image region to read
     */
    protected RegionDef getRegionDef(
            List<List<Integer>> resolutionLevels, PixelBuffer pixelBuffer) {
        log.debug("Setting region to read");
        int resolution =
                Optional.ofNullable(imageRegionCtx.resolution).orElse(0);
        int sizeX = resolutionLevels.get(resolution).get(0);
        int sizeY = resolutionLevels.get(resolution).get(1);
        RegionDef regionDef = new RegionDef();
        Dimension imageTileSize = pixelBuffer.getTileSize();
        if (imageRegionCtx.tile != null) {
            int tileSizeX = imageRegionCtx.tile.getWidth();
            int tileSizeY = imageRegionCtx.tile.getHeight();
            if (tileSizeX == 0) {
                tileSizeX = (int) imageTileSize.getWidth();
            }
            if (tileSizeX > maxTileLength) {
                tileSizeX = maxTileLength;
            }
            if (tileSizeY == 0) {
                tileSizeY = (int) imageTileSize.getHeight();
            }
            if (tileSizeY > maxTileLength) {
                tileSizeY = maxTileLength;
            }
            regionDef.setWidth(tileSizeX);
            regionDef.setHeight(tileSizeY);
            regionDef.setX(imageRegionCtx.tile.getX() * tileSizeX);
            regionDef.setY(imageRegionCtx.tile.getY() * tileSizeY);
        } else if (imageRegionCtx.region != null) {
            regionDef.setX(imageRegionCtx.region.getX());
            regionDef.setY(imageRegionCtx.region.getY());
            regionDef.setWidth(imageRegionCtx.region.getWidth());
            regionDef.setHeight(imageRegionCtx.region.getHeight());
        } else {
            regionDef.setX(0);
            regionDef.setY(0);
            regionDef.setWidth(sizeX);
            regionDef.setHeight(sizeY);
            return regionDef;
        }
        truncateRegionDef(sizeX, sizeY, regionDef);
        flipRegionDef(sizeX, sizeY, regionDef);
        return regionDef;
    }

    /**
     * Sets the pyramid resolution level on the <code>renderingEngine</code>
     * @param renderer fully initialized renderer
     * @param resolutionLevels complete definition of all resolution levels for
     * the image.
     */
    private void setResolutionLevel(
            Renderer renderer, List<List<Integer>> resolutionLevels) {
        log.debug("Number of available resolution levels: {}",
                resolutionLevels.size());

        if (imageRegionCtx.resolution != null) {
            log.debug("Setting resolution level: {}",
                    imageRegionCtx.resolution);
            Integer level =
                    resolutionLevels.size() - imageRegionCtx.resolution - 1;
            log.debug("Setting resolution level to: {}", level);
            renderer.setResolutionLevel(level);
        }
    }

    /**
     *  Splits an hex stream of characters into an array of bytes
     *  in format (R,G,B,A).
     *  - abc      -> (0xAA, 0xBB, 0xCC, 0xFF)
     *  - abcd     -> (0xAA, 0xBB, 0xCC, 0xDD)
     *  - abbccd   -> (0xAB, 0xBC, 0xCD, 0xFF)
     *  - abbccdde -> (0xAB, 0xBC, 0xCD, 0xDE)
     *  @param color: Characters to split.
     *  @return rgba - list of Ints
     */
    public static int[] splitHTMLColor(String color) {
        List<Integer> level1 = Arrays.asList(3, 4);
        int[] out = new int[4];
        try {
            if (level1.contains(color.length())) {
                String c = color;
                color = "";
                for (char ch : c.toCharArray()) {
                    color += ch + ch;
                }
            }
            if (color.length() == 6) {
                color += "FF";
            }
            if (color.length() == 8) {
                out[0] = Integer.parseInt(color.substring(0, 2), 16);
                out[1] = Integer.parseInt(color.substring(2, 4), 16);
                out[2] = Integer.parseInt(color.substring(4, 6), 16);
                out[3] = Integer.parseInt(color.substring(6, 8), 16);
                return out;
            }
        } catch (Exception e) {
            log.error("Error while parsing color: {}", color, e);
        }
        return null;
    }
}
