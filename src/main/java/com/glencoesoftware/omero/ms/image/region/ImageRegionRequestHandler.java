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

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ServiceRegistry;
import javax.imageio.stream.ImageOutputStream;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriter;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriterSpi;

import io.vertx.core.Future;
import ome.api.local.LocalCompress;
import ome.io.nio.InMemoryPlanarPixelBuffer;
import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import ome.model.core.Image;
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
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

public class ImageRegionRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionRequestHandler.class);

    /** OMERO server Spring application context. */
    private final ApplicationContext context;

    /** OMERO server pixels service. */
    private final PixelsService pixelsService;

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

    /** Pixels metadata */
    private Pixels pixels;

    /** Get Pixel Buffer Summary */
    private static final Summary getPixelBufferSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("get_pixel_buffer")
      .help("Get Pixel Buffer time")
      .register();

    /** Render As Int Summary */
    private static final Summary renderAsPackedIntSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("render_as_packed_int")
      .help("Render as packed int time")
      .register();

    /** Render Image Region Summary */
    private static final Summary renderImageRegionSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("render_image_region")
      .help("Render image region")
      .register();

    /** Load Pixels Summary */
    private static final Summary loadPixelsSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("load_pixels")
      .help("load pixles time")
      .register();

    /** Retrieve Pixel Description Summary */
    private static final Summary retrievePixSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("retrieve_pixels")
      .help("retrieve pixles time")
      .register();

    /** Render Summary */
    private static final Summary renderSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("render")
      .help("render time")
      .register();

    /** Get Pixels ID and Serise Summary */
    private static final Summary getPixIdAndSeriesSummary= Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("get_pix_id_and_series")
      .help("Get pixels ID and series time")
      .register();

    /** Project Stack Summary */
    private static final Summary projectStackSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("project_stack")
      .help("Project Stack time")
      .register();

    /** Can Read Async Summary */
    private static final Summary canReadAsyncSummary = Summary.build()
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .name("can_read_async_rh")
      .help("Can Read time")
      .register();

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx, ApplicationContext context,
            List<Family> families, List<RenderingModel> renderingModels,
            LutProvider lutProvider) {
        this.imageRegionCtx = imageRegionCtx;
        this.context = context;
        this.families = families;
        this.renderingModels = renderingModels;
        this.lutProvider = lutProvider;

        pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");
        projectionService = new ProjectionService();
        compressionSrv =
                (LocalCompress) context.getBean("internal-ome.api.ICompress");
    }

    /**
     * Render Image region request handler.  {@link #setPixels(Pixels)} must be
     * called before calling this method either with pixels metadata loaded
     * using {@link #loadPixels(omero.client)} or from cache.
     * @return A response body in accordance with the initial settings
     * provided by <code>imageRegionCtx</code>.
     * @see #setPixels(Pixels)
     * @see #loadPixels(omero.client)
     */
    public byte[] renderImageRegion() {
        Summary.Timer renderImageRegionTimer =
            renderImageRegionSummary.startTimer();
        try {
            if (pixels != null) {
                return getRegion(pixels);
            }
            log.debug("Cannot find Image:{}", imageRegionCtx.imageId);
        } catch (Exception e) {
            log.error("Exception while retrieving image region", e);
        } finally {
            renderImageRegionTimer.observeDuration();
        }
        return null;
    }

    /**
     * Retrieves a single {@link Pixels} identifier and Bio-Formats series from
     * the server for a given {@link Image} or <code>null</code> if no such
     * identifier exists or the user does not have permissions to access it.
     * @param iQuery OMERO query service to use for metadata access.
     * @param imageId {@link Image} identifier to query for.
     * @return See above.
     * @throws ServerError
     */
    private PixelsIdAndSeries getPixelsIdAndSeries(
            IQueryPrx iQuery, Long imageId)
                    throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageId);
        Summary.Timer timer = getPixIdAndSeriesSummary.startTimer();
        try {
            List<List<omero.RType>> data = iQuery.projection(
                    "SELECT p.id, p.image.series FROM Pixels as p " +
                    "WHERE p.image.id = :id",
                    params, ctx
                );
            if (data.size() != 1) {
                return null;
            }
            List<omero.RType> row = data.get(0);
            if (row.size() != 2) {
                return null;
            }
            return new PixelsIdAndSeries(
                    ((omero.RLong) row.get(0)).getValue(),
                    ((omero.RInt) row.get(1)).getValue());
        } finally {
            timer.observeDuration();
        }
    }

    /**
     * Creates a new set of rendering settings corresponding to the specified
     * pixels set.  Most values will be overwritten during
     * {@link #updateSettings(Renderer)} usage.
     * @param pixels pixels metadata
     * @return See above.
     */
    private RenderingDef createRenderingDef(Pixels pixels) throws ServerError {
        QuantumFactory quantumFactory = new QuantumFactory(families);
        StatsFactory statsFactory = new StatsFactory();

        RenderingDef renderingDef = new RenderingDef();
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

    /**
     * Retrieves a pixel buffer for the specified pixels set.
     * @param pixels pixels metadata
     * @return See above.
     * @throws ApiUsageException
     */
    private PixelBuffer getPixelBuffer(Pixels pixels)
            throws ApiUsageException {
        Summary.Timer timer = getPixelBufferSummary.startTimer();
        try {
            return pixelsService.getPixelBuffer(pixels, false);
        } finally {
            timer.observeDuration();
        }
    }

    /**
     * Retrieves the current active pixels metadata for the request.
     * @return See above.
     */
    public Pixels getPixels() {
        return pixels;
    }

    /**
     * Sets the current active pixels metadta for the request.
     * @param pixels pixels metadata loaded using
     * {@link #loadPixels(omero.client)
     */
    public void setPixels(Pixels pixels) {
        this.pixels = pixels;
    }

    /**
     * Retrieves pixels metadata from the server.
     * @param client OMERO client to use for querying.
     * @return Populated {@link Pixels} ready to be used by the
     * {@link Renderer} or <code>null</code> if it cannot be retrieved.
     * @see #setPixels(Pixels)
     */
    public Pixels loadPixels(omero.client client) {
        Summary.Timer timer = loadPixelsSummary.startTimer();
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            IPixelsPrx iPixels = sf.getPixelsService();
            PixelsIdAndSeries pixelsIdAndSeries = getPixelsIdAndSeries(
                    iQuery, imageRegionCtx.imageId);
            if (pixelsIdAndSeries == null) {
                return null;
            }

            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            Summary.Timer timer2 = retrievePixSummary.startTimer();
            Pixels pixels;
            try {
                pixels = (Pixels) mapper.reverse(iPixels.retrievePixDescription(
                        pixelsIdAndSeries.pixelsId, ctx));
                // The series will be used by our version of PixelsService which
                // avoids attempting to retrieve the series from the database
                // via IQuery later.
                Image image = new Image(pixels.getImage().getId(), true);
                image.setSeries(pixelsIdAndSeries.series);
                pixels.setImage(image);
                return pixels;
            } finally {
                timer2.observeDuration();
            }
        } catch (ApiUsageException e) {
            String v = "Illegal API usage while retrieving Pixels metadata";
            log.error(v, e);
        } catch (ServerError e) {
            String v = "Server error while retrieving Pixels metadata";
            log.error(v, e);
        } finally {
            timer.observeDuration();
        }
        return null;
    }

    /**
     * Retrieves a single region from the server in the requested format as
     * defined by <code>imageRegionCtx.format</code>.
     * @param pixels pixels metadata
     * @return Image region as a byte array.
     * @throws QuantizationException
     */
    private byte[] getRegion(Pixels pixels)
                    throws IllegalArgumentException, ServerError, IOException,
                    QuantizationException {
        log.debug("Getting image region");
        QuantumFactory quantumFactory = new QuantumFactory(families);
        PixelBuffer pixelBuffer = getPixelBuffer(pixels);

        renderer = new Renderer(
            quantumFactory, renderingModels,
            pixels, createRenderingDef(pixels),
            pixelBuffer, lutProvider
        );
        PlaneDef planeDef = new PlaneDef(PlaneDef.XY, imageRegionCtx.t);
        planeDef.setZ(imageRegionCtx.z);
        planeDef.setRegion(getRegionDef(pixels, pixelBuffer));

        List<List<Integer>> resolutionLevels =
                pixelBuffer.getResolutionDescriptions();
        setResolutionLevel(renderer, resolutionLevels);
        if (imageRegionCtx.compressionQuality != null) {
            compressionSrv.setCompressionLevel(
                    imageRegionCtx.compressionQuality);
        }
        updateSettings(renderer);
        Summary.Timer timer = renderSummary.startTimer();
        try {
            // The actual act of rendering will close the provided pixel buffer
            return render(renderer, resolutionLevels, pixels, planeDef);
        } finally {
            timer.observeDuration();
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
            Renderer renderer, List<List<Integer>> resolutionLevels,
            Pixels pixels, PlaneDef planeDef)
                    throws ServerError, IOException, QuantizationException {
        checkPlaneDef(resolutionLevels, planeDef);

        Summary.Timer timer = renderAsPackedIntSummary.startTimer();
        int[] buf;
        try {
            PixelBuffer newBuffer = null;
            if (imageRegionCtx.projection != null) {
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
                try {
                    for (int i = 0; i < channelBindings.length; i++) {
                        if (!channelBindings[i].getActive()) {
                            continue;
                        }
                        Summary.Timer timer2 = projectStackSummary.startTimer();
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
                            timer2.observeDuration();
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
            timer.observeDuration();
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
     * Copied from {@link RenderingBean#checkPlaneDef}. A client side version
     * of this is required when we are rendering uncompressed.
     * @param resolutionLevels complete definition of all resolution levels
     * for the image.
     * @param planeDef plane definition to validate
     * @throws ServerError
     */
    private void checkPlaneDef(
            List<List<Integer>> resolutionLevels, PlaneDef planeDef)
                    throws ServerError{
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
     * @throws ServerError
     */
    private void updateSettings(Renderer renderer) throws ServerError {
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
     * Returns RegionDef to read based on tile / region provided in
     * ImageRegionCtx.
     * @param pixels pixels metadata
     * @param pixelBuffer raw pixel data access buffer
     * @return RegionDef {@link RegionDef} describing image region to read
     * @throws IllegalArgumentException
     * @throws ServerError
     */
    private RegionDef getRegionDef(Pixels pixels, PixelBuffer pixelBuffer)
            throws IllegalArgumentException, ServerError {
        log.debug("Setting region to read");
        RegionDef regionDef = new RegionDef();
        if (imageRegionCtx.tile != null) {
            Dimension tileSize = pixelBuffer.getTileSize();
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
            regionDef.setWidth(pixels.getSizeX());
            regionDef.setHeight(pixels.getSizeY());
        }
        return regionDef;
    }

    /**
     * Sets the pyramid resolution level on the <code>renderingEngine</code>
     * @param renderer fully initialized renderer
     * @param resolutionLevels complete definition of all resolution levels for
     * the image.
     * @throws ServerError
     */
    private void setResolutionLevel(
            Renderer renderer,
            List<List<Integer>> resolutionLevels)
                    throws ServerError {
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

    /**
     * Whether or not a single {@link ImageI} can be read from the server.
     * @param client OMERO client to use for querying.
     * @return <code>true</code> if the {@link ImageI} can be loaded or
     * <code>false</code> otherwise.
     * @see #canReadAsync(omero.client)
     */
    public boolean canRead(omero.client client) {
        log.debug("Checking readability for Image:{}", imageRegionCtx.imageId);
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageRegionCtx.imageId);
        try {
            List<List<omero.RType>> rows = client.getSession()
                    .getQueryService().projection(
                            "SELECT i.id FROM Image as i " +
                            "WHERE i.id = :id", params, ctx);
            if (rows.size() > 0) {
                return true;
            }
        } catch (Exception e) {
            log.error("Exception while checking Image readability", e);
        }
        return false;
    }

    /**
     * Whether or not a single {@link ImageI} can be read from the server.
     * @param client OMERO client to use for querying.
     * @return Future with the same completion value semantics as
     * {@link #canRead(omero.client)}
     * @see #canRead(omero.client)
     */
    public Future<Boolean> canReadAsync(omero.client client) {
        log.debug("Checking readability for Image:{} asynchronously",
                imageRegionCtx.imageId);
        Future<Boolean> future = Future.future();

        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageRegionCtx.imageId);
        Summary.Timer timer = canReadAsyncSummary.startTimer();
        try {
            client.getSession().getQueryService().begin_projection(
                "SELECT i.id FROM Image as i WHERE i.id = :id",
                params, ctx, (List<List<omero.RType>> rows) -> {
                    timer.observeDuration();
                    if (rows.size() > 0) {
                        future.complete(true);
                    } else {
                        future.complete(false);
                    }
                }, (Ice.UserException e) -> {
                    timer.observeDuration();
                    log.error("Exception while checking Image readability", e);
                    future.complete(false);
                }, (Ice.Exception e) -> {
                    timer.observeDuration();
                    log.error("Exception while checking Image readability", e);
                    future.complete(false);
                });
        } catch (Exception e) {
            timer.observeDuration();
            log.error("Exception while checking Image readability", e);
            future.complete(false);
        }

        return future;
    }

    /**
     * Struct like class to store {@link ome.model.core.Pixels} and Bio-Formats
     * series metadata.
     */
    private final class PixelsIdAndSeries {

        /** {@link ome.model.core.Pixels} identifier */
        public final long pixelsId;

        /** Bio-Formats series */
        public final int series;

        PixelsIdAndSeries(long pixelsId, int series) {
            this.pixelsId = pixelsId;
            this.series = series;
        }

    }
}
