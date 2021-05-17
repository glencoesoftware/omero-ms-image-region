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
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.model.fs.Fileset;
import ome.util.ImageUtil;
import omeis.providers.re.Renderer;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumFactory;
import omero.ApiUsageException;
import omero.RType;
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

    /** Top-level directory containing NGFF files */
    protected final String ngffDir;

    /** Configured OmeroZarrUtils */
    protected final OmeroZarrUtils zarrUtils;

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
            PixelsService pixelsService,
            String ngffDir,
            OmeroZarrUtils zarrUtils) {
        this.compressionSrv = compressionSrv;
        this.lutProvider = lutProvider;
        this.families = families;
        this.renderingModels = renderingModels;
        this.pixelsService = pixelsService;
        this.ngffDir = ngffDir;
        this.zarrUtils = zarrUtils;
        this.imageRegionCtx = imageRegionCtx;
        this.maxTileLength = maxTileLength;
        projectionService = new ProjectionService();
    }

    /**
     * Retrieves a single {@link Pixels} identifier and Bio-Formats series from
     * the server for a given {@link Image} or <code>null</code> if no such
     * identifier exists or the user does not have permissions to access it.
     * @param iQuery OMERO query service to use for metadata access.
     * @param imageId {@link Image} identifier to query for.
     * @return See above.
     * @throws ServerError If there was any sort of error retrieving the pixels
     * id.
     */
    public List<RType> getPixelsIdAndSeries(
        IQueryPrx iQuery, Long imageId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageId);
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixels_id_and_series");
        span.tag("omero.image_id", imageId.toString());
        try {
            List<List<RType>> data = iQuery.projection(
                    "SELECT p.id, p.image.series FROM Pixels as p " +
                    "WHERE p.image.id = :id",
                    params, ctx
                );
            if (data.size() < 1) {
                return null;
            }
            return data.get(0);  // The first row
        } finally {
            span.finish();
        }
    }

    /**
     * Get Pixels information from ID
     * @param pixelsIdAndSeries ID and Series for this Pixels object
     * @param mapper IceMapper
     * @param iPixels Pixels proxy service
     * @param iQuery Query proxy service
     * @return Populated Pixels object
     * @throws ApiUsageException
     * @throws ServerError
     */
    public Pixels retrievePixDescription(
        List<RType> pixelsIdAndSeries,
        IceMapper mapper, IPixelsPrx iPixels, IQueryPrx iQuery)
                throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_pix_description");
        Pixels pixels;
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            long pixelsId =
                    ((omero.RLong) pixelsIdAndSeries.get(0)).getValue();
            span.tag("omero.pixels_id", Long.toString(pixelsId));
            pixels = (Pixels) mapper.reverse(
                    iPixels.retrievePixDescription(pixelsId, ctx));
            // The series will be used by our version of PixelsService which
            // avoids attempting to retrieve the series from the database
            // via IQuery later.
            Image image = new Image(pixels.getImage().getId(), true);
            image.setFileset(new Fileset(getFilesetIdFromImageId(
                    iQuery, pixels.getImage().getId()), true));
            image.setSeries(((omero.RInt) pixelsIdAndSeries.get(1)).getValue());
            pixels.setImage(image);
            return pixels;
        } finally {
            span.finish();
        }
    }

    public long getFilesetIdFromImageId(IQueryPrx iQuery, Long imageId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageId);
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_fileset_id_from_image_id");
        span.tag("omero.image_id", imageId.toString());
        try {
            omero.model.Image image = (omero.model.Image) iQuery.findByQuery(
                    "SELECT i from Image i " +
                    "WHERE i.id = :id",
                    params, ctx
                );
            return image.getFileset().getId().getValue();
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
    public RenderingDef getRenderingDef(IPixelsPrx iPixels, final long pixelsId)
                throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        return (RenderingDef) mapper.reverse(
                iPixels.retrieveRndSettings(pixelsId, ctx));
    }

    /**
     * Copied from {@link RenderingBean#checkPlaneDef}. A client side version
     * of this is required when we are rendering uncompressed.
     * @param resolutionLevels complete definition of all resolution levels
     * for the image.
     * @param planeDef plane definition to validate
     * @throws ServerError
     */
    public void checkPlaneDef(
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
            List<RType> pixelsIdAndSeries = getPixelsIdAndSeries(
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

    protected void updateSettings(Renderer renderer) {
        imageRegionCtx.updateSettings(renderer, families, renderingModels);
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
    protected byte[] getRegion(
            IQueryPrx iQuery, IPixelsPrx iPixels, List<RType> pixelsIdAndSeries)
                    throws IllegalArgumentException, ServerError, IOException,
                    QuantizationException {
        Pixels pixels = retrievePixDescription(
                pixelsIdAndSeries, mapper, iPixels, iQuery);
        return compress(render(pixels, iPixels));
    }

    protected byte[] compress(Array array) throws IOException {
        Integer sizeY = array.getShape()[0];
        Integer sizeX = array.getShape()[1];
        int[] buf = (int[]) array.getStorage();
        buf = flip(buf, sizeX, sizeY,
                imageRegionCtx.flipHorizontal, imageRegionCtx.flipVertical);
        BufferedImage image = ImageUtil.createBufferedImage(
            buf, sizeX, sizeY
        );
        return compress(image);
    }

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
            updateSettings(renderer);
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
                    span.tag(
                            "omero.rendering_stats",
                            renderer.getStats().getStats());
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
    protected void truncateRegionDef(
            int sizeX, int sizeY, RegionDef regionDef) {
        log.debug("Truncating RegionDef if required");
        if (regionDef.getX() > sizeX ||
                regionDef.getY() > sizeY) {
            throw new IllegalArgumentException(
                    String.format("Start position (%d,%d) exceeds image size (%d, %d)",regionDef.getX(),
                            regionDef.getY(), sizeX, sizeY));
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
