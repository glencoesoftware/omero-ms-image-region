package com.glencoesoftware.omero.ms.image.region;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;


import brave.ScopedSpan;
import brave.Tracing;
import ome.io.nio.PixelBuffer;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.display.RenderingDef;
import ome.model.fs.Fileset;
import omeis.providers.re.Renderer;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omero.ApiUsageException;
import omero.RType;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class RenderingUtils {

    /** OMERO server pixels service. */
    private final PixelsService pixelsService;

    private final String ngffDir;

    private final TiledbUtils tiledbUtils;

    private final OmeroZarrUtils zarrUtils;

    public RenderingUtils(PixelsService pixelsService,
            String ngffDir,
            TiledbUtils tiledbUtils,
            OmeroZarrUtils zarrUtils) {
        this.pixelsService = pixelsService;
        this.ngffDir = ngffDir;
        this.tiledbUtils = tiledbUtils;
        this.zarrUtils = zarrUtils;
    }

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(RenderingUtils.class);

    public static PixelBuffer getPixelBuffer(PixelsService pixelsService, Pixels pixels,
            String ngffDir, TiledbUtils tiledbUtils, OmeroZarrUtils zarrUtils) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixel_buffer");
        span.tag("omero.pixels_id", pixels.getId().toString());
        PixelBuffer pb = null;
        try {
            try {
                pb = pixelsService.getNgffPixelBuffer(pixels, ngffDir, tiledbUtils, zarrUtils);
            } catch(Exception e) {
                log.error("Error when getting TieldbPixelBuffer", e);
                log.info("Getting TiledbPixelBuffer failed - attempting to get local data");
            }
            if(pb == null) {
                pb = pixelsService.getPixelBuffer(pixels, false);
            }
            return pb;
        } finally {
            span.finish();
        }
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
    public static List<RType> getPixelsIdAndSeries(IQueryPrx iQuery, Long imageId)
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

    public static Pixels retrievePixDescription(List<RType> pixelsIdAndSeries,
            IceMapper mapper, IPixelsPrx iPixels, IQueryPrx iQuery) throws ApiUsageException, ServerError {
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
            image.setFileset(new Fileset(getFilesetIdFromImageId(iQuery, pixels.getImage().getId()), true));
            image.setSeries(((omero.RInt) pixelsIdAndSeries.get(1)).getValue());
            pixels.setImage(image);
            return pixels;
        } finally {
            span.finish();
        }
    }

    public static long getFilesetIdFromImageId(IQueryPrx iQuery, Long imageId)
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


    public PixelBuffer getPixelBuffer(Pixels pixels) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixel_buffer");
        span.tag("omero.pixels_id", pixels.getId().toString());
        PixelBuffer pb = null;
        try {
            try {
                pb = pixelsService.getNgffPixelBuffer(pixels, ngffDir, tiledbUtils, zarrUtils);
            } catch(Exception e) {
                log.error("Error when getting TieldbPixelBuffer", e);
                log.info("Getting TiledbPixelBuffer failed - attempting to get local data");
            }
            if(pb == null) {
                pb = pixelsService.getPixelBuffer(pixels, false);
            }
            return pb;
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
    public static RenderingDef getRenderingDef(
            IPixelsPrx iPixels, final long pixelsId, IceMapper mapper) throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        return (RenderingDef) mapper.reverse(
                iPixels.retrieveRndSettings(pixelsId, ctx));
    }

    /**
     * Sets the pyramid resolution level on the <code>renderingEngine</code>
     * @param renderer fully initialized renderer
     * @param resolutionLevels complete definition of all resolution levels for
     * the image.
     * @throws ServerError
     */
    public static void setResolutionLevel(
            Renderer renderer,
            Integer resolutionLevelCount,
            Integer resolution)
                    throws ServerError {
        log.debug("Number of available resolution levels: {}",
                resolutionLevelCount);

        if (resolution != null) {
            log.debug("Setting resolution level: {}",
                    resolution);
            Integer level =
                    resolutionLevelCount - resolution - 1;
            log.debug("Setting resolution level to: {}", level);
            renderer.setResolutionLevel(level);
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
    public static void checkPlaneDef(
            Integer sizeX, Integer sizeY, PlaneDef planeDef)
                    throws ServerError{
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
