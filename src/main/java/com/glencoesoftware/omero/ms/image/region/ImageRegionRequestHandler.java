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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.lang.IllegalArgumentException;

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
import omero.ApiUsageException;
import omero.RType;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;

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

    /**
     * Default constructor.
     * @param imageRegionCtx {@link ImageRegionCtx} object
     */
    public ImageRegionRequestHandler(
            ImageRegionCtx imageRegionCtx, ApplicationContext context,
            List<Family> families, List<RenderingModel> renderingModels,
            LutProvider lutProvider) {
        log.info("Setting up handler");
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
     * Render Image region request handler.
     * @param client OMERO client to use for querying.
     * @return A response body in accordance with the initial settings
     * provided by <code>imageRegionCtx</code>.
     */
    public byte[] renderImageRegion(omero.client client) {
        StopWatch t0 = new Slf4JStopWatch("renderImageRegion");
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
            log.error("Exception while retrieving image region", e);
        } finally {
            t0.stop();
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
     * @throws ServerError If there was any sort of error retrieving the pixels
     * id.
     */
    private List<RType> getPixelsIdAndSeries(IQueryPrx iQuery, Long imageId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageId);
        StopWatch t0 = new Slf4JStopWatch("getPixelsIdAndSeries");
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
            t0.stop();
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
            IPixelsPrx iPixels, final long pixelsId) throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        return (RenderingDef) mapper.reverse(
                iPixels.retrieveRndSettings(pixelsId, ctx));
    }

    private PixelBuffer getPixelBuffer(Pixels pixels)
            throws ApiUsageException {
        StopWatch t0 = new Slf4JStopWatch("getPixelBuffer");
        try {
            return pixelsService.getPixelBuffer(pixels, false);
        } finally {
            t0.stop();
        }
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
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        StopWatch t0 = new Slf4JStopWatch(
                "PixelsService.retrievePixDescription");
        Pixels pixels;
        try {
            long pixelsId =
                    ((omero.RLong) pixelsIdAndSeries.get(0)).getValue();
            pixels = (Pixels) mapper.reverse(
                    iPixels.retrievePixDescription(pixelsId, ctx));
            // The series will be used by our version of PixelsService which
            // avoids attempting to retrieve the series from the database
            // via IQuery later.
            Image image = new Image(pixels.getImage().getId(), true);
            image.setSeries(((omero.RInt) pixelsIdAndSeries.get(1)).getValue());
            pixels.setImage(image);
        } finally {
            t0.stop();
        }
        QuantumFactory quantumFactory = new QuantumFactory(families);
        try (PixelBuffer pixelBuffer = getPixelBuffer(pixels)) {
            renderer = new Renderer(
                quantumFactory, renderingModels,
                pixels, getRenderingDef(iPixels, pixels.getId()),
                pixelBuffer, lutProvider
            );
            PlaneDef planeDef = new PlaneDef(PlaneDef.XY, imageRegionCtx.t);
            planeDef.setZ(imageRegionCtx.z);
            planeDef.setRegion(getRegionDef(pixels, pixelBuffer));

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
            setResolutionLevel(renderer, resolutionLevels);
            if (imageRegionCtx.compressionQuality != null) {
                compressionSrv.setCompressionLevel(
                        imageRegionCtx.compressionQuality);
            }
            updateSettings(renderer);
            StopWatch t1 = new Slf4JStopWatch("render");
            try {
                // The actual act of rendering will close the provided pixel
                // buffer.  However, just in case an exception is thrown before
                // reaching this point a double close may occur due to the
                // surrounding try-with-resources block.
                return render(renderer, resolutionLevels, pixels, planeDef);
            } finally {
                t1.stop();
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
            Renderer renderer, List<List<Integer>> resolutionLevels,
            Pixels pixels, PlaneDef planeDef)
                    throws ServerError, IOException, QuantizationException {
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
        buf = mirror(buf, sizeX, sizeY,
                imageRegionCtx.mirrorX, imageRegionCtx.mirrorY);
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
     * Mirror an image over the X, Y or both axes.
     * @param src source image buffer
     * @param sizeX size of <code>src</code> in X (number of columns)
     * @param sizeY size of <code>src</code> in Y (number of rows)
     * @param mirrorX whether or not to mirror the image over the X axis
     * @param mirrorY whether or not to mirror the image over the Y axis
     * @return Newly allocated buffer with mirroring applied or <code>src</code>
     * if no mirroring has been requested.
     */
    public static int[] mirror(
            int[] src, int sizeX, int sizeY, boolean mirrorX, boolean mirrorY) {
        if (!mirrorX && !mirrorY) {
            return src;
        }

        if (src == null) {
            throw new IllegalArgumentException("Attempted to mirror null image");
        } else if (sizeX == 0 || sizeY == 0) {
            throw new IllegalArgumentException("Attempted to mirror image with 0 size");
        }

        int[] dest = new int[src.length];
        int srcIndex, destIndex;
        int xOffset = mirrorX? sizeX : 1;
        int yOffset = mirrorY? sizeY : 1;
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

    public static RegionDef getMirroredRegionDef(int imageSizeX,
        int imageSizeY,
        int tileSizeX,
        int tileSizeY,
        int x, //Tile X position
        int y, //Tile Y position
        boolean mirrorX,
        boolean mirrorY) {
        log.debug("Getting mirrored tile RegionDef");
        RegionDef regionDef = new RegionDef();
        int edgeSizeX = imageSizeX % tileSizeX;
        int edgeSizeY = imageSizeY % tileSizeY;
        //Calculate number of tiles
        int numTilesX = imageSizeX/tileSizeX;
        if (edgeSizeX != 0)
            numTilesX += 1;
        int numTilesY = imageSizeY/tileSizeY;
        if (edgeSizeY != 0)
            numTilesY += 1;
        if (mirrorX) {
            regionDef.setX(numTilesX - 1 - x);
            //If post-mirror we will have the last tile, it may not be full
            if (x == 0 && edgeSizeX != 0) {
                regionDef.setWidth(imageSizeX % tileSizeX);
            } else { //Just a standard size tile
                regionDef.setWidth(tileSizeX);
            }
        } else{
            regionDef.setX(x);
            //If it's the last tile, it may not be full
            if (x == imageSizeX/tileSizeX - 1 && edgeSizeX != 0) {
                regionDef.setWidth(edgeSizeX);
            } else {
                regionDef.setWidth(tileSizeX);
            }
        }
        if (mirrorY) {
            regionDef.setY(numTilesY - 1 - y);
            //If post-mirror we will have the last tile, it may not be full
            if (y == 0 && edgeSizeY != 0) {
                regionDef.setHeight(edgeSizeY);
            } else { //Just a standard size tile
                regionDef.setHeight(tileSizeY);
            }
        } else {
            regionDef.setY(y);
            //If it's the last tile, it may not be full
            if (y == imageSizeY/tileSizeY - 1 && edgeSizeY != 0) {
                regionDef.setHeight(edgeSizeY);
            } else {
                regionDef.setHeight(tileSizeY);
            }
        }
        return regionDef;
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
        Dimension tileSize = pixelBuffer.getTileSize();
        regionDef = getMirroredRegionDef(pixels.getSizeX(),
            pixels.getSizeX(),
            (int) tileSize.getWidth(),
            (int) tileSize.getHeight(),
            regionDef.getX() / regionDef.getWidth(), //Tile X position
            regionDef.getY() / regionDef.getHeight(), //Tile Y position
            imageRegionCtx.mirrorX,
            imageRegionCtx.mirrorY);
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
}
