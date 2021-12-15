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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.lang.IllegalArgumentException;
import java.lang.Math;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ServiceRegistry;
import javax.imageio.stream.ImageOutputStream;

import org.slf4j.LoggerFactory;

import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriter;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriterSpi;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import ome.api.local.LocalCompress;
import ome.io.nio.InMemoryPlanarPixelBuffer;
import ome.io.nio.PixelBuffer;
import ome.logic.PixelsImpl;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.util.ImageUtil;
import omeis.providers.re.Renderer;
import omeis.providers.re.RenderingStats;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumFactory;
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;
import ucar.ma2.Array;
import ucar.ma2.DataType;

import static omero.rtypes.rlong;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    /** Reference to the compression service. */
    protected final LocalCompress compressionSrv;

    /** Lookup table provider. */
    protected final LutProvider lutProvider;

    /** Available families */
    protected final List<Family> families;

    /** Available rendering models */
    protected final List<RenderingModel> renderingModels;

    /** OMERO server pixels service. */
    protected final PixelsService pixelsService;

    /** Reference to the projection service. */
    private final ProjectionService projectionService;

    /** Image Region Context */
    private final ImageRegionCtx imageRegionCtx;

    /** Configured maximum size size in either dimension */
    private final int maxTileLength;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects.
     */
    protected final IceMapper mapper = new IceMapper();

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            LocalCompress compressionSrv,
            int maxTileLength,
            PixelsService pixelsService) {
        this.compressionSrv = compressionSrv;
        this.lutProvider = lutProvider;
        this.families = families;
        this.renderingModels = renderingModels;
        this.pixelsService = pixelsService;
        this.imageRegionCtx = imageRegionCtx;
        this.maxTileLength = maxTileLength;
        projectionService = new ProjectionService();
    }

    /**
     * Get Pixels information from Image IDs
     * @param imageIds Image IDs to get Pixels information for
     * @param iQuery Query proxy service
     * @return Map of Image ID vs. Populated Pixels object
     * @throws ApiUsageException
     * @throws ServerError
     */
    protected Map<Long, Pixels> retrievePixDescription(
            IQueryPrx iQuery, List<Long> imageIds)
                throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_pix_description");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_ids", imageIds.toString());
            // Query pulled from ome.logic.PixelsImpl and expanded to include
            // our required Image / Plate metadata; loading both sides of the
            // Image <--> WellSample <--> Well collection so that we can
            // resolve our field index.
            ParametersI params = new ParametersI();
            params.addIds(imageIds);
            List<Pixels> pixelsList = (List<Pixels>) mapper.reverse(
                    iQuery.findAllByQuery(
                        "select p from Pixels as p "
                        + "join fetch p.image as i "
                        + "left outer join fetch i.wellSamples as ws "
                        + "left outer join fetch ws.well as w "
                        + "left outer join fetch w.wellSamples "
                        + "join fetch p.pixelsType "
                        + "join fetch p.channels as c "
                        + "join fetch c.logicalChannel as lc "
                        + "left outer join fetch c.statsInfo "
                        + "left outer join fetch lc.photometricInterpretation "
                        + "left outer join fetch lc.illumination "
                        + "left outer join fetch lc.mode "
                        + "left outer join fetch lc.contrastMethod "
                        + "where i.id in (:ids)", params, ctx));
            Map<Long, Pixels> toReturn = new HashMap<Long, Pixels>();
            for (Pixels pixels : pixelsList) {
                toReturn.put(pixels.getImage().getId(), pixels);
            }
            return toReturn;
        } finally {
            span.finish();
        }
    }

    /**
     * Selects the correct rendering settings either from the user
     * (preferred) or image owner corresponding to the specified
     * pixels set.
     * @param renderingDefs A list of rendering settings to select from.
     * @param pixelsId The identifier of the pixels.
     * @return See above.
     */
    protected RenderingDef selectRenderingDef(
            List<RenderingDef> renderingDefs, final long userId,
            final long pixelsId)
                throws ServerError {
        RenderingDef userRenderingDef = renderingDefs
            .stream()
            .filter(v -> v.getPixels().getId() == pixelsId)
            .filter(v -> v.getDetails().getOwner().getId() == userId)
            .findFirst()
            .orElse(null);
        if (userRenderingDef != null) {
            return userRenderingDef;
        }
        // Otherwise pick the first (from the owner) if available
        return renderingDefs
                .stream()
                .filter(v -> v.getPixels().getId() == pixelsId)
                .findFirst()
                .orElse(null);
    }

    /**
     * Gets the correct rendering settings either from the user (preferred) or
     * image owner corresponding to the specified pixels set.
     * @param client OMERO client to use for querying.
     * @param pixelsId The identifier of the pixels.
     * @return See above.
     */
    protected RenderingDef getRenderingDef(
            omero.client client, final long pixelsId)
                throws ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_rendering_def");
        try {
            ServiceFactoryPrx sf = client.getSession();
            long userId = sf.getAdminService().getEventContext().userId;
            List<RenderingDef> renderingDefs = retrieveRenderingDefs(
                    client, userId, Arrays.asList(pixelsId));
            return selectRenderingDef(renderingDefs, userId, pixelsId);
        } catch (Exception e) {
            span.error(e);
        } finally {
            span.finish();
        }
        return null;
    }

    /**
     * Retrieves rendering settings either from the user or image owner
     * corresponding to any of the specified pixels sets.
     * @param client OMERO client to use for querying.
     * @param userId The current user ID.
     * @param pixelsIds The pixels set identifiers.
     * @return See above.
     */
    protected List<RenderingDef> retrieveRenderingDefs(
            omero.client client, final long userId, final List<Long> pixelsIds)
                throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_rendering_defs");
        // Ask for rendering settings for the current user or the image owner
        String q = PixelsImpl.RENDERING_DEF_QUERY_PREFIX
                + "rdef.pixels.id in (:ids) "
                + "and ("
                + "  rdef.details.owner.id = rdef.pixels.details.owner.id"
                + "    or rdef.details.owner.id = :userId"
                + ")";
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            ParametersI params = new ParametersI();
            params.addIds(pixelsIds);
            params.add("userId", rlong(userId));
            return (List<RenderingDef>) mapper.reverse(
                            iQuery.findAllByQuery(q, params, ctx));
        } catch (Exception e) {
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Copied from {@link RenderingBean#checkPlaneDef}. A client side version
     * of this is required when we are rendering uncompressed.
     * @param resolutionLevels complete definition of all resolution levels
     * for the image.
     * @param planeDef plane definition to validate
     * @throws ServerError
     */
    private void checkPlaneDef(
            Integer sizeX, Integer sizeY, PlaneDef planeDef){
        RegionDef rd = planeDef.getRegion();
        if (rd == null) {
            return;
        }
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
     * Render Image region request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>imageRegionCtx</code>.
     */
    public byte[] renderImageRegion(omero.client client) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("render_image_region");
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            Map<Long, Pixels> imagePixels = retrievePixDescription(
                    iQuery, Arrays.asList(imageRegionCtx.imageId));
            Pixels pixels = imagePixels.get(imageRegionCtx.imageId);
            if (pixels != null) {
                RenderingDef renderingDef =
                        getRenderingDef(client, pixels.getId());
                return getRegion(client, pixels, renderingDef);
            }
            log.debug("Cannot find Image:{}", imageRegionCtx.imageId);
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while retrieving image region", e);
        } finally {
            span.finish();
        }
        return null;
    }

    /**
     * Retrieves a single region from the server in the requested format as
     * defined by <code>imageRegionCtx.format</code>.
     * @param client OMERO client to use for querying.
     * @param pixels pixels metadata
     * @param renderingDef rendering settings to use
     * @return Image region as a byte array.
     * @throws QuantizationException
     */
    private byte[] getRegion(
            omero.client client, Pixels pixels, RenderingDef renderingDef)
                    throws IllegalArgumentException, ServerError, IOException,
                        QuantizationException {
        return compress(getBufferedImage(render(client, pixels, renderingDef)));
    }

    /**
     * Get a {@link BufferedImage} of the data in the given array
     * @param array wrapped pixel data with contextual dimensional extents and
     * pixel type encoded
     * @return BufferedImage of the data
     * @throws IOException
     */
    protected BufferedImage getBufferedImage(Array array) throws IOException {
        Integer sizeY = array.getShape()[0];
        Integer sizeX = array.getShape()[1];
        int[] buf = (int[]) array.getStorage();
        buf = flip(buf, sizeX, sizeY,
                imageRegionCtx.flipHorizontal, imageRegionCtx.flipVertical);
        return ImageUtil.createBufferedImage(
            buf, sizeX, sizeY
        );
    }

    /**
     * Compress rendered pixel data in accordance with the current
     * <code>imageRegionCtx</code>.
     * @param image buffered image of the correct dimensions to compress
     * @return Compressed pixel data ready to return to the client.
     * @throws IOException
     */
    protected byte[] compress(BufferedImage image) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] toReturn = null;
        String format = imageRegionCtx.format;
        if (format.equals("jpeg")) {
            compressionSrv.compressToStream(image, output);
            toReturn = output.toByteArray();
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
            toReturn = output.toByteArray();
        } else {
            log.error("Unknown format {}", imageRegionCtx.format);
        }
        return toReturn;
    }

    /**
     * Returns a pixel buffer for a given set of pixels.
     * @param pixels pixels metadata
     * @return See above.
     * @see PixelsService#getPixelBuffer(Pixels)
     */
    private PixelBuffer getPixelBuffer(Pixels pixels) {
        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan("get_pixel_buffer");
        try {
            span.tag("omero.pixels_id", Long.toString(pixels.getId()));
            return pixelsService.getPixelBuffer(pixels, false);
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    /**
     * Prepares an in memory pixel buffer of the desired project pixels based
     * on input.
     * @param pixels pixels metadata
     * @param renderer fully initialized renderer
     * @return See above.
     * @throws IOException if there is an error reading from or closing the
     * original pixel buffer configured in the renderer
     */
    private PixelBuffer prepareProjectedPixelBuffer(
            Pixels pixels, Renderer renderer) throws IOException {
        byte[][][][] planes = new byte[1][pixels.getSizeC()][1][];
        int projectedSizeC = 0;
        ChannelBinding[] channelBindings =
                renderer.getChannelBindings();
        PixelBuffer pixelBuffer = getPixelBuffer(pixels);
        int start = Optional
                .ofNullable(imageRegionCtx.projectionStart)
                .orElse(0);
        int end = Optional
                .ofNullable(imageRegionCtx.projectionEnd)
                .orElse(pixels.getSizeZ() - 1);
        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan(
                "prepare_projected_pixel_buffer");
        try {
            for (int i = 0; i < channelBindings.length; i++) {
                if (!channelBindings[i].getActive()) {
                    continue;
                }
                ScopedSpan span2 =
                        tracer.startScopedSpan("project_stack");
                span2.tag("omero.pixels_id", pixels.getId().toString());
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
                    span2.finish();
                }
                projectedSizeC++;
            }
        } finally {
            try {
                pixelBuffer.close();
            } finally {
                span.finish();
            }
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
        return new InMemoryPlanarPixelBuffer(projectedPixels, planes);
    }

    /**
     * Performs conditional rendering.
     * @param client OMERO client to use for querying.
     * @param pixels pixels metadata
     * @param renderingDef rendering settings to use
     * @return Image region as packed integer array of shape [Y, X] ready for
     * compression.
     * @throws ServerError
     * @throws IOException
     * @throws QuantizationException
     */
    protected Array render(
            omero.client client, Pixels pixels, RenderingDef renderingDef)
                    throws ServerError, IOException, QuantizationException {
        QuantumFactory quantumFactory = new QuantumFactory(families);

        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan("render_as_packed_int");
        span.tag("omero.pixels_id", pixels.getId().toString());
        Renderer renderer = null;
        try (PixelBuffer pixelBuffer = getPixelBuffer(pixels)) {
            renderer = new Renderer(
                quantumFactory, renderingModels, pixels, renderingDef,
                pixelBuffer, lutProvider
            );
            int t = Optional.ofNullable(imageRegionCtx.t)
                    .orElse(renderingDef.getDefaultT());
            int z = Optional.ofNullable(imageRegionCtx.z)
                    .orElse(renderingDef.getDefaultZ());
            PlaneDef planeDef = new PlaneDef(PlaneDef.XY, t);
            planeDef.setZ(z);

            // Avoid asking for resolution descriptions if there is no image
            // pyramid.  This can be *very* expensive.
            imageRegionCtx.setResolutionLevel(pixelBuffer);
            Integer sizeX = pixelBuffer.getSizeX();
            Integer sizeY = pixelBuffer.getSizeY();
            RegionDef regionDef = imageRegionCtx.getRegionDef(pixelBuffer, maxTileLength);
            planeDef.setRegion(regionDef);
            checkPlaneDef(sizeX, sizeY, planeDef);

            if (imageRegionCtx.compressionQuality != null) {
                compressionSrv.setCompressionLevel(
                        imageRegionCtx.compressionQuality);
            }
            imageRegionCtx.updateSettings(renderer, families, renderingModels);
            PixelBuffer newBuffer = null;
            if (imageRegionCtx.projection != null) {
                newBuffer = prepareProjectedPixelBuffer(pixels, renderer);
                planeDef = new PlaneDef(PlaneDef.XY, 0);
                planeDef.setZ(0);
            }
            // RegionDef is updated by the rendering operation to the reflect
            // the size of the resulting array and consequently must happen
            // first.
            int[] buf = renderer.renderAsPackedInt(planeDef, newBuffer);
            return Array.factory(
                    DataType.INT,
                    new int[] { regionDef.getHeight(), regionDef.getWidth() },
                    buf);
        } finally {
            try {
                if (renderer != null) {
                    RenderingStats stats = renderer.getStats();
                    if (stats != null) {
                        span.tag("omero.rendering_stats", stats.getStats());
                    }
                }
            } finally {
                span.finish();
            }
        }
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
    protected int[] flip(
            int[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        if (!flipHorizontal && !flipVertical) {
            return src;
        }

        if (src == null) {
            throw new IllegalArgumentException("Attempted to flip null image");
        } else if (sizeX == 0 || sizeY == 0) {
            throw new IllegalArgumentException(
                    "Attempted to flip image with 0 size");
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
}
