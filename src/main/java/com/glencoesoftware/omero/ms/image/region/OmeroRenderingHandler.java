package com.glencoesoftware.omero.ms.image.region;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import ome.api.local.LocalCompress;
import ome.io.nio.PixelBuffer;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.model.fs.Fileset;
import ome.xml.model.primitives.Color;
import omeis.providers.re.Renderer;
import omeis.providers.re.codomain.ReverseIntensityContext;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omero.ApiUsageException;
import omero.RType;
import omero.ServerError;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class OmeroRenderingHandler extends OmeroVertxRequestHandler{

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(OmeroRenderingHandler.class);

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

    public OmeroRenderingHandler(
            LocalCompress compressionSrv,
            LutProvider lutProvider,
            List<Family> families,
            List<RenderingModel> renderingModels,
            PixelsService pixelsService,
            String ngffDir,
            OmeroZarrUtils zarrUtils
            ) {
        this.compressionSrv = compressionSrv;
        this.lutProvider = lutProvider;
        this.families = families;
        this.renderingModels = renderingModels;
        this.pixelsService = pixelsService;
        this.ngffDir = ngffDir;
        this.zarrUtils = zarrUtils;
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
    *
    * @param pixelsService Configured PixelsService
    * @param pixels Pixels set to retrieve a pixel buffer for.
    * @param ngffDir Top-level directory containing NGFF files
    * @param zarrUtils Configured OmeroZarrUtils
    * @return NGFF or standard PixelBuffer
    */
    public PixelBuffer getPixelBuffer(Pixels pixels) {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("get_pixel_buffer");
        span.tag("omero.pixels_id", pixels.getId().toString());
        PixelBuffer pb = null;
        try {
            try {
                pb = pixelsService.getNgffPixelBuffer(
                        pixels, ngffDir, zarrUtils);
            } catch(Exception e) {
                log.info(
                    "Getting NGFF Pixel Buffer failed - " +
                    "attempting to get local data");
            }
            if (pb == null) {
                pb = pixelsService.getPixelBuffer(pixels, false);
            }
            return pb;
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
    public static Pixels retrievePixDescription(
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

    /**
     * Retrieves the rendering settings corresponding to the specified pixels
     * set.
     * @param iPixels OMERO pixels service to use for metadata access.
     * @param pixelsId The identifier of the pixels.
     * @return See above.
     */
    public static RenderingDef getRenderingDef(
        IPixelsPrx iPixels, final long pixelsId, IceMapper mapper)
                throws ServerError {
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
}
