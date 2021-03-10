package com.glencoesoftware.omero.ms.image.region;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ome.api.IScale;
import ome.api.local.LocalCompress;
import ome.io.nio.PixelBuffer;
import ome.logic.PixelsImpl;
import ome.model.core.Pixels;
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
import omero.model.IObject;
import omero.model.Image;
import omero.model.RenderingDef;
import omero.model.ChannelBinding;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class ThumbnailsRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailsRequestHandler.class);

    ThumbnailCtx thumbnailCtx;

    /** Rendering helper class */
    protected final RenderingUtils renderingUtils;

    /** Reference to the compression service. */
    private final LocalCompress compressionSrv;

    /** Lookup table provider. */
    private final LutProvider lutProvider;

    /** Available families */
    private final List<Family> families;

    /** Available rendering models */
    private final List<RenderingModel> renderingModels;

    /** Scaling service */
    private final IScale iScale;

    /** Interface for NGFF operations */
    private final NgffUtils ngffUtils;

    /** On-Disk or cloud location for NGFF files */
    private final String ngffDir;

    /** Longest side of the thumbnail. */
    protected final int longestSide;

    /** Image identifiers to request thumbnails for. */
    protected final List<Long> imageIds;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects.
     */
    private final IceMapper mapper = new IceMapper();

    /** Renderer */
    private Renderer renderer;

    /**
     * Default constructor
     * @param thumbnailCtx ThumbnailCtx object
     * @param renderingUtils Configured RenderingUtils
     * @param compressionSrv Compression service
     * @param families List of families
     * @param renderingModels List of renering models
     * @param lutProvider Lookup table provider
     * @param iScale Scaling service
     * @param ngffUtils Configured NgffUtils
     * @param ngffDir Location (local or cloud) of NGFF files
     * @param longestSide Longest side of the final thumbnail
     * @param imageIds imageIds {@link Image} identifiers to get thumbnails for
     */
    public ThumbnailsRequestHandler(
            ThumbnailCtx thumbnailCtx,
            RenderingUtils renderingUtils,
            LocalCompress compressionSrv,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            IScale iScale,
            NgffUtils ngffUtils,
            String ngffDir,
            int longestSide,
            List<Long> imageIds) {
        this.thumbnailCtx = thumbnailCtx;
        this.renderingUtils = renderingUtils;
        this.compressionSrv = compressionSrv;
        this.lutProvider = lutProvider;
        this.families = families;
        this.renderingModels = renderingModels;
        this.iScale = iScale;
        this.ngffUtils = ngffUtils;
        this.ngffDir = ngffDir;
        this.longestSide = longestSide;
        this.imageIds = imageIds;
    }

    /**
     * Retrieves a map of JPEG thumbnails from the server.
     * @return Map of {@link Image} identifier to JPEG thumbnail byte array.
     */
    public Map<Long, byte[]> renderThumbnails(omero.client client) {
        try {
            ServiceFactoryPrx sf = client.getSession();
            long userId = sf.getAdminService().getEventContext().userId;
            IQueryPrx iQuery = sf.getQueryService();
            IPixelsPrx iPixels = sf.getPixelsService();
            List<Image> images = getImages(client, imageIds);
            if (images.size() != 0) {
                return getThumbnails(
                        iQuery, iPixels, userId, images, longestSide);
            } else {
                log.debug("Cannot find any Images with Ids {}", imageIds);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving thumbnails", e);
        }
        return null;
    }

    /**
     * Retrieves a list of loaded {@link Image}s from the server.
     * @param client OMERO client to use for querying.
     * @param imageIds {@link Image} identifiers to query for.
     * @return List of loaded {@link Image} and primary {@link Pixels}.
     * @throws ServerError If there was any sort of error retrieving the images.
     */
    protected List<Image> getImages(omero.client client, List<Long> imageIds)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addIds(imageIds);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_images");
        try {
            return client.getSession().getQueryService().findAllByQuery(
                "SELECT i FROM Image as i " +
                "JOIN FETCH i.pixels as p WHERE i.id IN (:ids)",
                params, ctx
            ).stream().map(x -> (Image) x).collect(Collectors.toList());
        } finally {
            span.finish();
        }
    }

    /**
     * Retrieves a byte array of rendered pixel data for the thumbnail
     * @param iQuery The query service proxy
     * @param iPixels The pixels service proxy
     * @param userId The Omero user ID
     * @param image The Image the use wants a thumbnail of
     * @param longestSide The longest side length of the final thumbnail
     * @return Byte array of jpeg thumbnail data
     * @throws IOException
     */
    protected byte[] getThumbnail(
        IQueryPrx iQuery, IPixelsPrx iPixels, Long userId,
        Image image, int longestSide)
            throws IOException {
        try {
            List<RType> pixelsIdAndSeries = RenderingUtils.getPixelsIdAndSeries(
                iQuery, image.getId().getValue());
            return getRegion(iQuery, iPixels, pixelsIdAndSeries, userId, null);
        } catch (Exception e) {
            log.error("Error getting thumbnail " + Long.toString(image.getId().getValue()), e);
            return new byte[0];
        }
    }

    /**
     * Retrieves a map of JPEG thumbnails from ngffDir.
     * @param images {@link Image} list to retrieve thumbnails for.
     * @param longestSide Size to confine or upscale the longest side of each
     * thumbnail to. The other side will then proportionately, based on aspect
     * ratio, be scaled accordingly.
     * @return Map of {@link Image} identifier to JPEG thumbnail byte array.
     * @throws IOException
     */
    protected Map<Long, byte[]> getThumbnails(
        IQueryPrx iQuery, IPixelsPrx iPixels, Long userId,
        List<Image> images, int longestSide)
            throws IOException {
        Map<Long, byte[]> thumbnails = new HashMap<Long, byte[]>();
        for (Image image : images) {
            thumbnails.put(
                    image.getId().getValue(),
                    getThumbnail(iQuery, iPixels, userId, image, longestSide));
        }
        return thumbnails;
    }

    /**
     *
     * @param iQuery OMERO query service to use for metadata access.
     * @param iPixels OMERO pixels service to use for metadata access.
     * @param pixelsIdAndSeries {@link Pixels} identifier and Bio-Formats series
     * @param userId ID of user making the request
     * @param renderingDefId Specific Rendering Def ID to use for this thumbnail
     * @return Thumbnail region as a byte array
     * @throws IllegalArgumentException
     * @throws ServerError
     * @throws IOException
     * @throws QuantizationException
     */
    protected byte[] getRegion(
        IQueryPrx iQuery, IPixelsPrx iPixels, List<RType> pixelsIdAndSeries,
        long userId, Optional<Long> renderingDefId)
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
            PlaneDef planeDef = new PlaneDef(PlaneDef.XY, 0);
            planeDef.setZ(0);

            List<List<Integer>> resDescriptions =
                    pixelBuffer.getResolutionDescriptions();
            log.info("Resolution level count: " + Integer.toString(resDescriptions.size()));
            int resolutionLevel = getResolutionForThumbnail(resDescriptions);
            RenderingUtils.setResolutionLevel(
                    renderer, resDescriptions.size(), resolutionLevel);
            Integer sizeX = resDescriptions.get(resolutionLevel).get(0);
            Integer sizeY = resDescriptions.get(resolutionLevel).get(1);
            RegionDef regionDef = new RegionDef();
            regionDef.setX(0);
            regionDef.setY(0);
            regionDef.setWidth(sizeX);
            regionDef.setHeight(sizeY);
            log.info(regionDef.toString());
            planeDef.setRegion(regionDef);
            updateRenderingSettings(iQuery, pixels, userId, renderingDefId);
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
     * Updates the settings in the rendering engine based on the user's
     * settings if they exist and the NGFF settings otherwise
     * @param iQuery The Query Service proxy for finding user's rendering def
     * @param pixels The pixels object of the image
     * @param userId The user requesting the thumbnail
     * @param renderingDefId Optional specific rendering def ID to use with this
     * thumbnail
     */
    private void updateRenderingSettings(
            IQueryPrx iQuery, Pixels pixels, long userId,
            Optional<Long> renderingDefId) {
        //Check for client-provided rendering def, then user def,
        //then pixels owner def, then NGFF def
        Set<Long> pixelsIds = new HashSet<Long>();
        pixelsIds.add(pixels.getId());
        ParametersI p;
        String sql;
        if (renderingDefId.isPresent()) {
            log.info("Using rendering def " + Long.toString(renderingDefId.get()));
            p = new ParametersI();
            List<Long> idList = new ArrayList<Long>();
            idList.add(renderingDefId.get());
            p.addIds(idList);
            sql = PixelsImpl.RENDERING_DEF_QUERY_PREFIX
                    + "rdef.id in (:ids)";
        } else if (userId >= 0) {
            // Load the rendering settings of the specified owner
            p = new ParametersI();
            p.addIds(pixelsIds);
            p.addId(userId);
            sql = PixelsImpl.RENDERING_DEF_QUERY_PREFIX
                    + "rdef.pixels.id in (:ids) and "
                    + "rdef.details.owner.id = :id";
        } else {
            // Load the rendering settings of the pixels owner
            p = new ParametersI();
            p.addIds(pixelsIds);

            sql = PixelsImpl.RENDERING_DEF_QUERY_PREFIX
                    + "rdef.pixels.id in (:ids) and "
                    + "rdef.details.owner.id = rdef.pixels.details.owner.id";
        }
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        try {
            List<IObject> settingsList = iQuery.findAllByQuery(sql, p, ctx);
            List<Integer> channels = new ArrayList<Integer>();
            List<Double[]> windows = new ArrayList<Double[]>();
            List<Integer[]> colors = new ArrayList<Integer[]>();
            if (settingsList.size() > 0) {
                RenderingDef rdef = (RenderingDef) settingsList.get(0);
                int numCbs = rdef.sizeOfWaveRendering();
                for (int i = 0; i < numCbs; i++) {
                    ChannelBinding cb = rdef.getChannelBinding(i);
                    if (cb.getActive().getValue()) {
                        channels.add(i+1);
                        Double[] window = new Double[] {
                                cb.getInputStart().getValue(),
                                cb.getInputEnd().getValue()};
                        windows.add(window);
                        Integer[] rgba = new Integer[] {
                                cb.getRed().getValue(),
                                cb.getGreen().getValue(),
                                cb.getBlue().getValue(),
                                cb.getAlpha().getValue()};
                        colors.add(rgba);
                    }
                }
            }
            if (channels.size() > 0) {
                log.info("Updating rendering settings from user settings");
                RenderingUtils.updateSettingsIntColors(
                        renderer, channels, windows, colors, null,
                        renderingModels, "rgb");
                return;
            }
        } catch (ServerError e) {
            log.error("Error getting rendering setttings from server", e);
        }

        //If not, load rendering settings from NGFF Metadata
        List<Integer> channels = new ArrayList<Integer>();
        List<Double[]> windows = new ArrayList<Double[]>();
        List<String> colors = new ArrayList<String>();
        JsonObject omeroMetadata = ngffUtils.getOmeroMetadata(
                ngffDir, pixels.getImage().getFileset().getId(),
                pixels.getImage().getSeries());
        if (omeroMetadata != null) {
            channels.clear();
            windows.clear();
            colors.clear();
            JsonArray ngffChannels = omeroMetadata.getJsonArray("channels");
            for (int i = 0; i < ngffChannels.size(); i++) {
                JsonObject channelInfo = ngffChannels.getJsonObject(i);
                if (channelInfo.getBoolean("active")) {
                    channels.add(i+1);
                    JsonObject window = channelInfo.getJsonObject("window");
                    windows.add(new Double[] {
                            Double.valueOf(window.getFloat("start")),
                            Double.valueOf(window.getFloat("end"))});
                    colors.add(channelInfo.getString("color"));
                }
            }
        } else {
            throw new IllegalArgumentException(String.format(
                    "NGFF Fileset %d missing omero metadata",
                    pixels.getImage().getFileset().getId()));
        }
        log.info("Updating rendering settings from NGFF metadata");
        RenderingUtils.updateSettings(
                renderer, channels, windows, colors, null,
                renderingModels, "rgb");
    }

    /**
     * Renders the thumbail as a jpeg
     * @param renderer fully initialized renderer
     * @param sizeX X size of the image (not final thumbnail)
     * @param sizeY Y size of the image (not final thumbnail)
     * @param pixels pixels metadata
     * @param planeDef plane definition to use for rendering
     * @return
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
            buf =  renderer.renderAsPackedInt(planeDef, newBuffer);
        } finally {
            span1.tag("omero.rendering_stats", renderer.getStats().getStats());
            span1.finish();
        }

        RegionDef region = planeDef.getRegion();
        sizeX = region != null? region.getWidth() : pixels.getSizeX();
        sizeY = region != null? region.getHeight() : pixels.getSizeY();
        BufferedImage image = ImageUtil.createBufferedImage(
            buf, sizeX, sizeY
        );
        Integer sizeToScale = sizeX > sizeY ? sizeX : sizeY;
        float scale = ((float) longestSide) / ((float) sizeToScale);
        image = iScale.scaleBufferedImage(image, scale, scale);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        compressionSrv.compressToStream(image, output);
        return output.toByteArray();
    }

    /**
     * Calculate the first resolution level larger than the thumbnail
     * @param resolutionDescriptions
     * @return
     */
    private int getResolutionForThumbnail(
            List<List<Integer>> rds) {
        int resolutionLevel = 0;
        for (; resolutionLevel < rds.size(); resolutionLevel++) {
            if (rds.get(resolutionLevel).get(0) < longestSide
                && rds.get(resolutionLevel).get(1) < longestSide) {
                break;
            }
        }
        resolutionLevel -= 1;
        if (resolutionLevel < 0) {
            throw new IllegalArgumentException(
                    "longestSide exceeds image size");
        }
        return resolutionLevel;
    }

}
