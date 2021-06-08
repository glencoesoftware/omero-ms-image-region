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
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;
import ucar.ma2.Array;
import ucar.ma2.DataType;

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
     * Retrieves the rendering settings corresponding to the specified pixels
     * set.
     * @param iPixels OMERO pixels service to use for metadata access.
     * @param pixelsId The identifier of the pixels.
     * @return See above.
     */
    private RenderingDef getRenderingDef(
            IPixelsPrx iPixels, final long pixelsId)
                throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_rendering_def");
        try {
            return (RenderingDef) mapper.reverse(
                    iPixels.retrieveRndSettings(pixelsId, ctx));
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
            IPixelsPrx iPixels = sf.getPixelsService();
            Map<Long, Pixels> imagePixels = retrievePixDescription(
                    iQuery, Arrays.asList(imageRegionCtx.imageId));
            Pixels pixels = imagePixels.get(imageRegionCtx.imageId);
            if (pixels != null) {
                return getRegion(iPixels, pixels);
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
     * @param iPixels OMERO pixels service to use for metadata access.
     * @param pixels pixels metadata
     * @return Image region as a byte array.
     * @throws QuantizationException
     */
    protected byte[] getRegion(IPixelsPrx iPixels, Pixels pixels)
            throws IllegalArgumentException, ServerError, IOException,
                QuantizationException {
        return compress(getBufferedImage(render(pixels, iPixels)));
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
        PixelBuffer pixelBuffer = pixelsService.getPixelBuffer(pixels, false);
        int start = Optional
                .ofNullable(imageRegionCtx.projectionStart)
                .orElse(0);
        int end = Optional
                .ofNullable(imageRegionCtx.projectionEnd)
                .orElse(pixels.getSizeZ() - 1);
        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan(
                "prepare_porjected_pixel_buffer");
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
     * @param pixels pixels metadata
     * @param iPixels OMERO pixels service to use for metadata access.
     * @return Image region as packed integer array of shape [Y, X] ready for
     * compression.
     * @throws ServerError
     * @throws IOException
     * @throws QuantizationException
     */
    protected Array render(Pixels pixels, IPixelsPrx iPixels)
            throws ServerError, IOException, QuantizationException {
        QuantumFactory quantumFactory = new QuantumFactory(families);

        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan("render_as_packed_int");
        span.tag("omero.pixels_id", pixels.getId().toString());
        Renderer renderer = null;
        try (PixelBuffer pixelBuffer =
                pixelsService.getPixelBuffer(pixels, false)) {
            RenderingDef rDef = getRenderingDef(iPixels, pixels.getId());
            renderer = new Renderer(
                quantumFactory, renderingModels, pixels, rDef,
                pixelBuffer, lutProvider
            );
            int t = Optional.ofNullable(imageRegionCtx.t)
                    .orElse(rDef.getDefaultT());
            int z = Optional.ofNullable(imageRegionCtx.z)
                    .orElse(rDef.getDefaultZ());
            PlaneDef planeDef = new PlaneDef(PlaneDef.XY, t);
            planeDef.setZ(z);

            // Avoid asking for resolution descriptions if there is no image
            // pyramid.  This can be *very* expensive.
            imageRegionCtx.setResolutionLevel(renderer, pixelBuffer);
            Integer sizeX = pixels.getSizeX();
            Integer sizeY = pixels.getSizeY();
            RegionDef regionDef = getRegionDef(sizeX, sizeY, pixelBuffer);
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
     * Update RegionDef to fit within the image boundaries.
     * @param sizeX width of the image at the current resolution
     * @param sizeY height of the image at the current resolution
     * @param regionDef region definition to truncate if required
     * @throws IllegalArgumentException
     * @see ImageRegionRequestHandler#getRegionDef(Pixels, PixelBuffer)
     */
    private void truncateRegionDef(
            int sizeX, int sizeY, RegionDef regionDef) {
        log.debug("Truncating RegionDef if required");
        if (regionDef.getX() > sizeX ||
                regionDef.getY() > sizeY) {
            throw new IllegalArgumentException(String.format(
                    "Start position (%d,%d) exceeds image size (%d, %d)",
                    regionDef.getX(), regionDef.getY(), sizeX, sizeY));
        }
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
    private void flipRegionDef(int sizeX, int sizeY, RegionDef regionDef) {
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
     * @throws IllegalArgumentException
     * @throws ServerError
     */
    protected RegionDef getRegionDef(
            Integer sizeX, Integer sizeY, PixelBuffer pixelBuffer)
                    throws IllegalArgumentException, ServerError {
        log.debug("Setting region to read");
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
