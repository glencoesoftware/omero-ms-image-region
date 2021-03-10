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
import java.util.List;
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
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.util.ImageUtil;
import omeis.providers.re.Renderer;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumFactory;
import omero.RType;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.util.IceMapper;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    /** Reference to the compression service. */
    private final LocalCompress compressionSrv;

    /** Reference to the projection service. */
    private final ProjectionService projectionService;

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

    /** Configured maximum size size in either dimension */
    private final int maxTileLength;

    private final RenderingUtils renderingUtils;

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            LocalCompress compSrv,
            int maxTileLength,
            RenderingUtils renderingUtils) {
        log.info("Setting up handler");
        this.imageRegionCtx = imageRegionCtx;
        this.families = families;
        this.renderingModels = renderingModels;
        this.lutProvider = lutProvider;
        this.maxTileLength = maxTileLength;
        this.renderingUtils = renderingUtils;

        projectionService = new ProjectionService();
        compressionSrv = compSrv;
    }

    /**
     * Render Image region request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>imageRegionCtx</code>.
     */
    public byte[] renderImageRegion(omero.client client) {
        log.info("renderImageRegion");
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("render_image_region");
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            IPixelsPrx iPixels = sf.getPixelsService();
            List<RType> pixelsIdAndSeries = RenderingUtils.getPixelsIdAndSeries(
                    iQuery, imageRegionCtx.imageId);
            if (pixelsIdAndSeries != null && pixelsIdAndSeries.size() == 2) {
                return getRegion(iQuery, iPixels, pixelsIdAndSeries);
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
     * @param iQuery OMERO query service to use for metadata access.
     * @param iPixels OMERO pixels service to use for metadata access.
     * @param pixelsAndSeries {@link Pixels} identifier and Bio-Formats series
     * to retrieve image region for.
     * @return Image region as a byte array.
     * @throws QuantizationException
     */
    private byte[] getRegion(
            IQueryPrx iQuery, IPixelsPrx iPixels, List<RType> pixelsIdAndSeries)
                    throws IllegalArgumentException, ServerError, IOException,
                    QuantizationException {
        log.debug("Getting image region");
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_pix_description");
        Pixels pixels = RenderingUtils.retrievePixDescription(
                pixelsIdAndSeries, mapper, iPixels, iQuery);
        QuantumFactory quantumFactory = new QuantumFactory(families);
        try (PixelBuffer pixelBuffer = renderingUtils.getPixelBuffer(pixels)) {
            log.info(pixelBuffer.toString());
            renderer = new Renderer(
                quantumFactory, renderingModels,
                pixels, RenderingUtils.getRenderingDef(
                        iPixels, pixels.getId(), mapper),
                pixelBuffer, lutProvider
            );
            PlaneDef planeDef = new PlaneDef(PlaneDef.XY, imageRegionCtx.t);
            planeDef.setZ(imageRegionCtx.z);

            // Avoid asking for resolution descriptions if there is no image
            // pyramid.  This can be *very* expensive.
            int countResolutionLevels = pixelBuffer.getResolutionLevels();
            log.info("Resolution level count: " + Integer.toString(countResolutionLevels));
            RenderingUtils.setResolutionLevel(
                    renderer, countResolutionLevels, imageRegionCtx.resolution);
            Integer sizeX = pixels.getSizeX();
            Integer sizeY = pixels.getSizeY();
            planeDef.setRegion(getRegionDef(sizeX, sizeY, pixelBuffer));
            if (imageRegionCtx.compressionQuality != null) {
                compressionSrv.setCompressionLevel(
                        imageRegionCtx.compressionQuality);
            }
            RenderingUtils.updateSettings(renderer,
                    imageRegionCtx.channels,
                    imageRegionCtx.windows,
                    imageRegionCtx.colors,
                    imageRegionCtx.maps,
                    renderingModels,
                    imageRegionCtx.m);
            span = Tracing.currentTracer().startScopedSpan("render");
            span.tag("omero.pixels_id", pixels.getId().toString());
            try {
                // The actual act of rendering will close the provided pixel
                // buffer.  However, just in case an exception is thrown before
                // reaching this point a double close may occur due to the
                // surrounding try-with-resources block.
                return render(renderer, sizeX, sizeY, pixels, planeDef);
            } finally {
                span.finish();
            }
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
     * @throws ServerError
     * @throws IOException
     * @throws QuantizationException
     */
    private byte[] render(
            Renderer renderer, Integer sizeX, Integer sizeY,
            Pixels pixels, PlaneDef planeDef)
                    throws ServerError, IOException, QuantizationException {
        RenderingUtils.checkPlaneDef(sizeX, sizeY, planeDef);

        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span1 = tracer.startScopedSpan("render_as_packed_int");
        span1.tag("omero.pixels_id", pixels.getId().toString());
        int[] buf;
        try {
            PixelBuffer newBuffer = null;
            if (imageRegionCtx.projection != null) {
                byte[][][][] planes = new byte[1][pixels.getSizeC()][1][];
                int projectedSizeC = 0;
                ChannelBinding[] channelBindings =
                        renderer.getChannelBindings();
                PixelBuffer pixelBuffer = renderingUtils.getPixelBuffer(pixels);
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
            span1.tag("omero.rendering_stats", renderer.getStats().getStats());
            span1.finish();
        }

        String format = imageRegionCtx.format;
        RegionDef region = planeDef.getRegion();
        sizeX = region != null? region.getWidth() : pixels.getSizeX();
        sizeY = region != null? region.getHeight() : pixels.getSizeY();
        buf = RenderingUtils.flip(buf, sizeX, sizeY,
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
        if (regionDef.getX() > sizeX ||
                regionDef.getY() > sizeY) {
            StringBuilder messageBuilder = new StringBuilder();
            messageBuilder.append("Start position (").append(regionDef.getX())
                .append(",").append(regionDef.getY())
                .append(") exceeds image size (").append(sizeX).append(",")
                .append(sizeY).append(")");
            throw new IllegalArgumentException(messageBuilder.toString());
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


}
