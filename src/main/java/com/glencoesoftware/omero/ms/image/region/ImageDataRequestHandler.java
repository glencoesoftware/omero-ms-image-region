package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import ome.logic.PixelsImpl;
import ome.model.core.Channel;
import ome.model.core.LogicalChannel;
import omero.model.Dataset;
import omero.model.Project;
import omero.model.Image;
import omero.model.Permissions;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.model.enums.UnitsLength;
import ome.model.stats.StatsInfo;
import ome.model.units.Length;
import ome.units.unit.Unit;
import omeis.providers.re.Renderer;
import omeis.providers.re.codomain.CodomainChain;
import omeis.providers.re.codomain.CodomainMapContext;
import omeis.providers.re.codomain.ReverseIntensityContext;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.quantum.QuantumFactory;
import omeis.providers.re.quantum.QuantumStrategy;
import omero.model.Details;
//import omero.model.Permissions;
import omero.model.Experimenter;
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IContainerPrx;
import omero.api.IQueryPrx;
import omero.api.RawPixelsStorePrx;
import omero.api.ServiceFactoryPrx;
import omero.model.IObject;
import omero.model.WellSampleI;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class ImageDataRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageDataRequestHandler.class);

    private ImageDataCtx imageDataCtx;

    private PixelsService pixelsService;

    private List<Family> families;

    List<RenderingModel> renderingModels;

    LutProvider lutProvider;
    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects.
     */
    protected final IceMapper mapper = new IceMapper();

    public ImageDataRequestHandler(ImageDataCtx imageDataCtx,
            PixelsService pixelsService,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider) {
        this.imageDataCtx = imageDataCtx;
        this.pixelsService = pixelsService;
        this.families = families;
        this.renderingModels = renderingModels;
        this.lutProvider = lutProvider;

    }

    public JsonObject getImageData(omero.client client) {
        log.info("In ReqeustHandler::getImageData");
        try {
            //testGetContainer(client);
            Long imageId = imageDataCtx.imageId;
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            Image image = queryImageData(iQuery, imageId);
            if (image == null) {
                return null;
            }
            Details details = image.getDetails();
            Experimenter owner = details.getOwner();

            log.info(image.toString());
            JsonObject imgData = new JsonObject();
            imgData.put("id", imageId);


            List<Long> imageIds = new ArrayList<Long>();
            imageIds.add(imageId);
            Long userId = sf.getAdminService().getEventContext().userId;
            Pixels pixels = retrievePixDescription(iQuery, imageIds).get(imageId);
            PixelBuffer pixelBuffer = getPixelBuffer(pixels);
            QuantumFactory quantumFactory = new QuantumFactory(families);
            List<Long> pixIds = new ArrayList<Long>();
            pixIds.add(pixels.getId());
            List<RenderingDef> rdefs = retrieveRenderingDefs(client, userId, pixIds);
            RenderingDef rdef = selectRenderingDef(rdefs, userId, pixels.getId());
            Renderer renderer = new Renderer(
                    quantumFactory, renderingModels, pixels, rdef,
                    pixelBuffer, lutProvider
                );

            JsonObject meta = new JsonObject();
            meta.put("imageName", image.getName().getValue());
            meta.put("imageDescription", image.getDescription().getValue());
            meta.put("imageAuthor", owner.getFirstName().getValue() + " " + owner.getLastName().getValue());
            //meta.put("imageAuthor", image.getAuthor())
            List<Dataset> datasets = image.linkedDatasetList();
            if(datasets.size() > 1) {
                meta.put("datasetName", "Multiple");
                Set<Long> projectIds = new HashSet<Long>();
                for(Dataset ds : datasets) {
                    List<Project> projects = ds.linkedProjectList();
                    if (projects.size() > 1) {
                        meta.put("projectName", "Multiple");
                        break;
                    } else {
                        if (projectIds.contains(projects.get(0).getId().getValue())) {
                            meta.put("projectName", "Multiple");
                            break;
                        } else {
                            projectIds.add(projects.get(0).getId().getValue());
                        }
                    }
                }
                if (!meta.containsKey("projectName")) {
                    Project project = datasets.get(0).linkedProjectList().get(0);
                    meta.put("projectName", project.getName());
                    meta.put("projectId", project.getId());
                    meta.put("projectDescription", project.getDescription());
                }
            } else if(datasets.size() == 1) {
                Dataset ds = datasets.get(0);
                meta.put("datasetName", ds.getName().getValue());
                meta.put("datasetId", ds.getId().getValue());
                meta.put("datasetDescription", ds.getDescription().getValue());
                List<Project> projects = ds.linkedProjectList();
                if (projects.size() > 1) {
                    meta.put("projectName", "Multiple");
                } else if (projects.size() == 1){
                    Project project = projects.get(0);
                    meta.put("projectName", project.getName().getValue());
                    meta.put("projectId", project.getId().getValue());
                    meta.put("projectDescription", project.getDescription().getValue());
                }

            }
            Optional<WellSampleI> wellSample = getWellSample(iQuery, imageId);
            if (wellSample.isPresent()) {
                meta.put("wellSampleId", wellSample.get().getId().getValue());
                meta.put("wellId", wellSample.get().getWell().getId().getValue());
            }
            meta.put("imageId", image.getId().getValue());
            meta.put("pixelsType", pixels.getPixelsType().getValue());
            imgData.put("meta", meta);


            Permissions permissions = details.getPermissions();
            JsonObject perms = new JsonObject();
            perms.put("canRead", true); //User would not have been able to load the image otherwise
            perms.put("canAnnotate", permissions.canAnnotate());
            perms.put("canWrite", permissions.canEdit());
            perms.put("canLink", permissions.canLink());
            imgData.put("perms", perms);


            int resLvlCount = pixelBuffer.getResolutionLevels();
            if (resLvlCount > 1) {
                imgData.put("tiles", true);
                JsonObject tileSize = new JsonObject();
                tileSize.put("width", pixelBuffer.getTileSize().width);
                tileSize.put("height", pixelBuffer.getTileSize().height);
                imgData.put("tile_size", tileSize);
            }
            imgData.put("levels", resLvlCount);

            JsonObject size = new JsonObject();
            size.put("width", pixelBuffer.getSizeX());
            size.put("height", pixelBuffer.getSizeY());
            size.put("z", pixelBuffer.getSizeZ());
            size.put("c", pixelBuffer.getSizeC());
            size.put("t", pixelBuffer.getSizeT());
            imgData.put("size", size);

            JsonObject pixelSize = new JsonObject();
            //Divide by units?
            pixelSize.put("x", Length.convertLength(pixels.getPhysicalSizeX(), UnitsLength.MICROMETER.getSymbol()).getValue());
            pixelSize.put("y", Length.convertLength(pixels.getPhysicalSizeY(), UnitsLength.MICROMETER.getSymbol()).getValue());
            pixelSize.put("z", pixels.getPhysicalSizeZ() != null ?
                    Length.convertLength(pixels.getPhysicalSizeZ(), UnitsLength.MICROMETER.getSymbol()).getValue() : null);
            imgData.put("pixel_size", pixelSize);

            imgData.put("init_zoom", 0);
            if (resLvlCount > 1) {
                JsonObject zoomLvlScaling = new JsonObject();
                List<List<Integer>> resDescs = renderer.getResolutionDescriptions();
                int maxXSize = resDescs.get(0).get(0);
                for (int i = 0; i < resLvlCount; i++) {
                    List<Integer> desc = resDescs.get(i);
                    zoomLvlScaling.put(Integer.toString(i), desc.get(0).doubleValue()/maxXSize);
                }
                imgData.put("zoomLevelScaling", zoomLvlScaling);
            }

            JsonArray pixelRange = new JsonArray();
            RawPixelsStorePrx rp = sf.createRawPixelsStore();
            try {
                Map<String, String> pixCtx = new HashMap<String, String>();
                pixCtx.put("omero.group", "-1");
                rp.setPixelsId(pixels.getId(), true, pixCtx);
                long pmax = Math.round(Math.pow(2, 8 * rp.getByteWidth()));
                if (rp.isSigned()) {
                    pixelRange.add(-1 * pmax / 2);
                    pixelRange.add(pmax / 2 - 1);
                } else {
                    pixelRange.add(0);
                    pixelRange.add(pmax -1 );
                }
                imgData.put("pixel_range", pixelRange);
            } finally {
                rp.close();
            }
            JsonArray channels = new JsonArray();
            int channelCount = pixels.sizeOfChannels();
            for (int i = 0; i < channelCount; i++) {
                Channel channel = pixels.getChannel(i);
                LogicalChannel logicalChannel = channel.getLogicalChannel();
                String label = null;
                logicalChannel.getName();
                if (logicalChannel.getName() != null && logicalChannel.getName().length() > 0) {
                    label = logicalChannel.getName();
                } else {
                    if (logicalChannel.getEmissionWave() != null) {
                        label = logicalChannel.getEmissionWave().toString();
                    } else {
                        label = Integer.toString(i);
                    }
                }
                log.info(channel.toString());
                JsonObject ch = new JsonObject();
                ch.put("emissionWave", logicalChannel.getEmissionWave() != null ?
                            logicalChannel.getEmissionWave().getValue() : null);
                ch.put("label", label);
                ch.put("color", getColorString(channel));
                ch.put("inverted", isInverted(renderer, i));
                ch.put("reverseIntensity", isInverted(renderer, i));
                ChannelBinding cb = renderer.getChannelBindings()[i];
                ch.put("family", cb.getFamily().getValue());
                ch.put("coefficient", cb.getCoefficient());
                ch.put("active", cb.getActive());
                StatsInfo statsInfo  = channel.getStatsInfo();
                JsonObject window = new JsonObject();
                if (statsInfo != null) {
                    window.put("min", statsInfo.getGlobalMin());
                    window.put("max", statsInfo.getGlobalMax());
                } else {
                    window.put("min", renderer.getPixelsTypeLowerBound(i));
                    window.put("max", renderer.getPixelsTypeUpperBound(i));
                }
                window.put("start", cb.getInputStart());
                window.put("end", cb.getInputEnd());
                ch.put("window", window);

                channels.add(ch);
            }
            imgData.put("channels", channels);
            JsonObject rd = new JsonObject();
            rd.put("model", rdef.getModel().getValue());
            rd.put("projection", rdef.sizeOfProjections() > 0 ? rdef.getPrimaryProjectionDef().toString() : null);
            rd.put("defaultZ", rdef.getDefaultZ());
            rd.put("defaultT", rdef.getDefaultT());

            JsonObject splitChannel = new JsonObject();
            int c = channelCount;
            JsonObject g = new JsonObject();
            // Greyscale, no channel overlayed image
            double x = Math.sqrt(c);
            long y = Math.round(x);
            long longX = 0;
            if (x > y) {
                longX = y+1;
            } else {
                longX = y;
            }
            int border = 2;
            g.put("width", pixels.getSizeX()*longX + border*(longX+1));
            g.put("height", pixels.getSizeY()*y+border*(y+1));
            g.put("border", border);
            g.put("gridx", x);
            g.put("gridy", y);
            splitChannel.put("g", g);
            JsonObject clr = new JsonObject();
            // Color, one extra image with all channels overlayed
            c += 1;
            x = Math.sqrt(c);
            y = Math.round(x);
            if (x > y) {
                longX = y+1;
            } else {
                longX = y;
            }
            clr.put("width", pixels.getSizeX()*longX + border*(longX+1));
            clr.put("height", pixels.getSizeY()*y+border*(y+1));
            clr.put("border", border);
            clr.put("gridx", x);
            clr.put("gridy", y);
            splitChannel.put("c", clr);
            imgData.put("split_channel", splitChannel);

            JsonObject rdefObj = new JsonObject();
            String rmodel = rdef.getModel().getValue().toLowerCase() == "greyscale" ?
                    "greyscale" : "color";
            rdefObj.put("model", rmodel);
            // "projection" and "invertAxis" are always going to be
            // "normal" and false in standard OMERO installs.
            rdefObj.put("projection", "normal");
            rdefObj.put("invertAxis", false);
            rdefObj.put("defaultZ", rdef.getDefaultZ());
            rdefObj.put("defaultT", rdef.getDefaultT());
            imgData.put("rdefs", rdefObj);

            return imgData;
        } catch (ServerError e) {
            log.error("Error getting image data");
        }
        return null;
    }

    private JsonObject getImageDataMeta(Image image,
            Pixels pixels,
            Experimenter owner,
            Optional<WellSampleI> wellSample) {
        JsonObject meta = new JsonObject();
        meta.put("imageName", image.getName().getValue());
        meta.put("imageDescription", image.getDescription().getValue());
        meta.put("imageAuthor", owner.getFirstName().getValue() + " " + owner.getLastName().getValue());
        List<Dataset> datasets = image.linkedDatasetList();
        if(datasets.size() > 1) {
            meta.put("datasetName", "Multiple");
            Set<Long> projectIds = new HashSet<Long>();
            for(Dataset ds : datasets) {
                List<Project> projects = ds.linkedProjectList();
                if (projects.size() > 1) {
                    meta.put("projectName", "Multiple");
                    break;
                } else {
                    if (projectIds.contains(projects.get(0).getId().getValue())) {
                        meta.put("projectName", "Multiple");
                        break;
                    } else {
                        projectIds.add(projects.get(0).getId().getValue());
                    }
                }
            }
            if (!meta.containsKey("projectName")) {
                Project project = datasets.get(0).linkedProjectList().get(0);
                meta.put("projectName", project.getName());
                meta.put("projectId", project.getId());
                meta.put("projectDescription", project.getDescription());
            }
        } else if(datasets.size() == 1) {
            Dataset ds = datasets.get(0);
            meta.put("datasetName", ds.getName().getValue());
            meta.put("datasetId", ds.getId().getValue());
            meta.put("datasetDescription", ds.getDescription().getValue());
            List<Project> projects = ds.linkedProjectList();
            if (projects.size() > 1) {
                meta.put("projectName", "Multiple");
            } else if (projects.size() == 1){
                Project project = projects.get(0);
                meta.put("projectName", project.getName().getValue());
                meta.put("projectId", project.getId().getValue());
                meta.put("projectDescription", project.getDescription().getValue());
            }

        }
        if (wellSample.isPresent()) {
            meta.put("wellSampleId", wellSample.get().getId().getValue());
            meta.put("wellId", wellSample.get().getWell().getId().getValue());
        }
        meta.put("imageId", image.getId().getValue());
        meta.put("pixelsType", pixels.getPixelsType().getValue());
        return meta;
    }

    private boolean isInverted(Renderer renderer, int channel) {
        CodomainChain chain = renderer.getCodomainChain(channel);
        List<CodomainMapContext> mapContexts = chain.getContexts();
        for (CodomainMapContext cmctx : mapContexts) {
            if (cmctx instanceof ReverseIntensityContext) {
                return true;
            }
        }
        return false;
    }

    private String getColorString(Channel channel) {
        StringBuilder colorBuilder = new StringBuilder();
        if (channel.getRed() < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(channel.getRed()).toUpperCase());
        if (channel.getGreen() < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(channel.getGreen()).toUpperCase());
        if (channel.getBlue() < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(channel.getBlue()).toUpperCase());
        return colorBuilder.toString();
    }

    protected Image queryImageData(
            IQueryPrx iQuery, Long imageId)
                throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_pix_description");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_id", imageId.toString());
            // Query pulled from ome.logic.PixelsImpl and expanded to include
            // our required Image / Plate metadata; loading both sides of the
            // Image <--> WellSample <--> Well collection so that we can
            // resolve our field index.
            ParametersI params = new ParametersI();
            params.addId(imageId);
            Image image = (Image)
                    iQuery.findByQuery(
                            "select i from Image as i " +
                            " join fetch i.pixels as pixels" +
                            " left outer JOIN FETCH i.datasetLinks as links " +
                            " left outer join fetch links.parent as dataset " +
                            " left outer join fetch dataset.projectLinks as plinks " +
                            " left outer join fetch plinks.parent as project " +
                            " where i.id=:id", params, ctx);
            return image;
        } finally {
            span.finish();
        }
    }

    public Optional<WellSampleI> getWellSample(IQueryPrx iQuery, Long imageId) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_wellsample");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            ParametersI params = new ParametersI();
            List<Long> ids = new ArrayList<Long>();
            ids.add(imageId);
            params.addIds(ids);
            List<IObject> wellSamples = iQuery.findAllByQuery(
                    "SELECT ws FROM WellSample AS ws" +
                    "  LEFT OUTER JOIN FETCH ws.well AS w" +
                    "  WHERE ws.image.id IN :ids",
                    params, ctx
                );
            for (IObject ob : wellSamples) {
                WellSampleI ws = (WellSampleI) ob;
                return Optional.of(ws);
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while retrieving image region", e);
        } finally {
            span.finish();
        }
        return Optional.empty();
    }

    public void testGetContainer(omero.client client) {
        ServiceFactoryPrx sf = client.getSession();
        try {
            IContainerPrx csvc = sf.getContainerService();
            ParametersI params = new ParametersI();
            List<Long> ids = new ArrayList<Long>();
            ids.add(251l);
            List<IObject> objs = csvc.loadContainerHierarchy("Project", ids, params);
            Project project = (Project) mapper.reverse(objs.get(0));
            log.info(project.toString());
        } catch (ServerError e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
            params.add("userId", omero.rtypes.rlong(userId));
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
}
