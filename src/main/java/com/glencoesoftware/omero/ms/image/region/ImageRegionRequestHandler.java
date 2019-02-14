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
import java.lang.IllegalArgumentException;
import java.lang.Math;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ServiceRegistry;
import javax.imageio.stream.ImageOutputStream;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriter;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriterSpi;

import io.vertx.core.json.Json;
import io.vertx.core.Vertx;
import ome.api.local.LocalCompress;
import ome.io.nio.InMemoryPlanarPixelBuffer;
import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
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
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumFactory;
import omero.util.IceMapper;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    public static final String IS_SESSION_VALID_EVENT =
            "omero.is_session_valid";

    public static final String CAN_READ_EVENT =
            "omero.can_read";

    public static final String GET_OBJECT_EVENT =
            "omero.get_object";

    public static final String GET_ALL_ENUMERATIONS_EVENT =
            "omero.get_all_enumerations";

    public static final String GET_RENDERING_SETTINGS_EVENT =
            "omero.get_rendering_settings";

    public static final String GET_PIXELS_DESCRIPTION_EVENT =
            "omero.get_pixels_description";

    /** OMERO server pixels service. */
    private final PixelsService pixelsService;
    
    /** Reference to the compression service. */
    private final LocalCompress compressionSrv;

    /** Lookup table provider. */
    private final LutProvider lutProvider;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects.
     */
    private final IceMapper mapper = new IceMapper();

    /** Image Region Context */
    private final ImageRegionCtx imageRegionCtx;

    /** Renderer */
    private Renderer renderer;

    /** Available families */
    private final List<Family> families;

    /** Available rendering models */
    private final List<RenderingModel> renderingModels;

    private final ProjectionService projectionService;

    /** Handle on Vertx for event bus work*/
    private Vertx vertx;

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx, ApplicationContext context,
            List<Family> families, List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            PixelsService pixService,
            LocalCompress compSrv,
            Vertx vertx) {
        log.info("Setting up handler");
        this.imageRegionCtx = imageRegionCtx;
        this.families = families;
        this.renderingModels = renderingModels;
        this.lutProvider = lutProvider;
        this.vertx = vertx;

        pixelsService = pixService;
        projectionService = new ProjectionService();
        compressionSrv = compSrv;
    }

    /**
     * Render Image region request handler.
     */
    public CompletableFuture<byte[]> renderImageRegion() {
        return getRegion();
    }

    /**
     * Retrieves the rendering settings corresponding to the specified pixels
     * set.
     * @param pixelsId The identifier of the pixels.
     * @return See above.
     */
    private CompletableFuture<RenderingDef> getRenderingDef(Long pixelsId) {
        CompletableFuture<RenderingDef> promise =
                new CompletableFuture<RenderingDef>();
        final Map<String, Object> data = new HashMap<String, Object>();
        data.put("sessionKey", imageRegionCtx.omeroSessionKey);
        data.put("pixelsId", pixelsId);
        StopWatch t0 = new Slf4JStopWatch(GET_RENDERING_SETTINGS_EVENT);
        vertx.eventBus().<byte[]>send(
                GET_RENDERING_SETTINGS_EVENT,
                Json.encode(data), result -> {
            String s = "";
            try {
                if (result.failed()) {
                    Throwable t = result.cause();
                    promise.completeExceptionally(t);
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                RenderingDef rd = (RenderingDef) ois.readObject();
                promise.complete(rd);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);
            } finally {
                t0.stop();
            }
        });

        return promise;
    }

    private CompletableFuture<RendererInfo> getRendererInfo(Pixels pixels) {
        return getRenderingDef(pixels.getId())
        .thenApply((renderingDef) -> {
            PixelBuffer pixelBuffer = getPixelBuffer(pixels);
            return new RendererInfo(pixels, pixelBuffer, renderingDef);
        });
    }

    private PixelBuffer getPixelBuffer(Pixels pixels) {
        Slf4JStopWatch t0 = new Slf4JStopWatch("PixelsService.getPixelBuffer");
        try {
            return pixelsService.getPixelBuffer(pixels,false);
        } finally {
            t0.stop();
        }
    }

    private CompletableFuture<Pixels> retrievePixDescription() {
        CompletableFuture<Pixels> promise = new CompletableFuture<>();
        final Map<String, Object> data = new HashMap<String, Object>();
        data.put("sessionKey", imageRegionCtx.omeroSessionKey);
        data.put("imageId", imageRegionCtx.imageId);
        StopWatch t0 = new Slf4JStopWatch(GET_PIXELS_DESCRIPTION_EVENT);
        vertx.eventBus().<byte[]>send(
                GET_PIXELS_DESCRIPTION_EVENT,
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
                Pixels pixels = (Pixels) ois.readObject();
                promise.complete(pixels);
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                promise.completeExceptionally(e);
            } finally {
                t0.stop();
            }
        });

        return promise;
    }

    private CompletableFuture<byte[]> getRegion() {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        return retrievePixDescription()
        .thenCompose(this::getRendererInfo)
        .thenApply((rendererInfo) -> {
            try {
                QuantumFactory quantumFactory = new QuantumFactory(families);
                RenderingDef renderingDef = rendererInfo.renderingDef;
                Pixels pixels = rendererInfo.pixels;
                PixelBuffer pixelBuffer = rendererInfo.PixelBuffer;
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
                    compressionSrv.setCompressionLevel(
                    imageRegionCtx.compressionQuality);
                }
                updateSettings(renderer);
                // The actual act of rendering will close the provided pixel
                // buffer.  However, just in case an exception is thrown before
                // reaching this point a double close may occur due to the
                // surrounding try-with-resources block.
                return render(
                        renderer, resolutionLevels, pixels, planeDef,
                        pixelBuffer);
            } catch (Exception e) {
                log.error("Error while rendering", e);
                return null;
            }
        });
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

        StopWatch t0 = new Slf4JStopWatch("Renderer.renderAsPackedInt");
        int[] buf;
        try {
            PixelBuffer newBuffer = null;
            if (imageRegionCtx.projection != null) {
                byte[][][][] planes = new byte[1][pixels.getSizeC()][1][];
                int projectedSizeC = 0;
                ChannelBinding[] channelBindings =
                        renderer.getChannelBindings();
                //PixelBuffer pixelBuffer = getPixelBuffer(pixels);
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
                        StopWatch t1 = new Slf4JStopWatch(
                                "ProjectionService.projectStack");
                        try {
                            planes[0][i][0] = projectionService.projectStack(
                                pixels,
                                pixelBuffer,
                                imageRegionCtx.projection.ordinal(),
                                imageRegionCtx.t,
                                i,  // Channel index
                                1,  // Stepping 1 in ImageWrapper.renderJpeg()
                                start,
                                end
                            );
                        } finally {
                            t1.stop();
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
            t0.stop();
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
            compressionSrv.compressToStream(image, output);
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
                    double min = (double) imageRegionCtx.windows.get(idx)[0];
                    double max = (double) imageRegionCtx.windows.get(idx)[1];
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
        Dimension tileSize = pixelBuffer.getTileSize();
        if (imageRegionCtx.tile != null) {
            regionDef.setWidth((int) tileSize.getWidth());
            regionDef.setHeight((int) tileSize.getHeight());
            regionDef.setX(imageRegionCtx.tile.getX() * regionDef.getWidth());
            regionDef.setY(imageRegionCtx.tile.getY() * regionDef.getHeight());
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
