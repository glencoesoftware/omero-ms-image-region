/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import omero.model.Channel;
import omero.model.LogicalChannel;
import omero.model.Dataset;
import omero.model.Project;
import omero.model.Image;
import omero.model.ImageI;
import omero.model.Permissions;
import omero.model.Pixels;
import omero.model.PixelsI;
import ome.model.display.ChannelBinding;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import ome.model.units.BigResult;
import omero.model.LengthI;
import omero.model.StatsInfo;
import omeis.providers.re.Renderer;
import omeis.providers.re.codomain.CodomainChain;
import omeis.providers.re.codomain.CodomainMapContext;
import omeis.providers.re.codomain.ReverseIntensityContext;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.quantum.QuantumFactory;
import omero.model.Details;
import omero.model.Event;
import omero.model.Experimenter;
import omero.ApiUsageException;
import omero.ServerError;
import omero.rtypes;
import omero.api.IContainerPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.IObject;
import omero.model.WellSampleI;
import omero.sys.ParametersI;
import omero.util.IceMapper;
import ome.units.UNITS;

import static omero.rtypes.unwrap;

public class ImageDataRequestHandler {

    private static final org.slf4j.Logger log = LoggerFactory
            .getLogger(ImageDataRequestHandler.class);

    private ImageDataCtx imageDataCtx;

    private PixelsService pixelsService;

    private List<Family> families;

    List<RenderingModel> renderingModels;

    LutProvider lutProvider;

    private int initZoom;

    private boolean interpolate;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects and
     * <code>ome.model</code> server side Hibernate backed objects.
     */
    protected final IceMapper mapper = new IceMapper();

    public ImageDataRequestHandler(ImageDataCtx imageDataCtx,
            PixelsService pixelsService, List<Family> families,
            List<RenderingModel> renderingModels, LutProvider lutProvider,
            int initZoom, boolean interpolate) {
        this.imageDataCtx = imageDataCtx;
        this.pixelsService = pixelsService;
        this.families = families;
        this.renderingModels = renderingModels;
        this.lutProvider = lutProvider;
        this.initZoom = initZoom;
        this.interpolate = interpolate;
    }

    public JsonObject getImageData(omero.client client) {
        log.info("In ReqeustHandler::getImageData");
        ServiceFactoryPrx sf = client.getSession();
        try {
            Long imageId = imageDataCtx.imageId;
            IQueryPrx iQuery = sf.getQueryService();
            ImageI image = queryImageData(iQuery, imageId);
            if (image == null) {
                return null;
            }
            Details details = image.getDetails();
            Experimenter owner = details.getOwner();
            PixelsI pixels = (PixelsI) image.getPrimaryPixels();
            List<Long> imageIds = new ArrayList<Long>();
            imageIds.add(imageId);
            Long userId = sf.getAdminService().getEventContext().userId;
            try (PixelBuffer pixelBuffer = getPixelBuffer(pixels)) {
                QuantumFactory quantumFactory = new QuantumFactory(families);
                List<Long> pixIds = new ArrayList<Long>();
                pixIds.add(pixels.getId().getValue());
                List<RenderingDef> rdefs = retrieveRenderingDefs(client, userId,
                        pixIds);
                RenderingDef rdef = selectRenderingDef(rdefs, userId,
                        pixels.getId().getValue());
                Renderer renderer = new Renderer(quantumFactory, renderingModels,
                        (ome.model.core.Pixels) mapper.reverse(pixels), rdef,
                        pixelBuffer, lutProvider);
                Permissions permissions = details.getPermissions();
                Event creationEvent = details.getCreationEvent();
                Map<String, String> pixCtx = new HashMap<String, String>();
                pixCtx.put("omero.group", "-1");
                return populateImageData(image, pixels, creationEvent,
                        owner, permissions, pixelBuffer, renderer,
                        rdef);
            }
        } catch (ServerError | IOException e) {
            log.error("Error getting image data", e);
        }
        return null;
    }

    public JsonObject populateImageData(Image image, PixelsI pixels,
            Event creationEvent, Experimenter owner, Permissions permissions,
            PixelBuffer pixelBuffer, Renderer renderer,
            RenderingDef rdef) {
        JsonObject imgData = new JsonObject();
        imgData.put("id", image.getId().getValue());
        JsonObject meta = getImageDataMeta(image, pixels, creationEvent,
                owner);
        imgData.put("meta", meta);
        if (image.getObjectiveSettings() != null) {
            imgData.put("nominalMagnification",
                    unwrap(image.getObjectiveSettings().getObjective()
                            .getNominalMagnification()));
        }

        JsonObject perms = getImageDataPerms(permissions);
        imgData.put("perms", perms);

        int resLvlCount = pixelBuffer.getResolutionLevels();
        if (resLvlCount > 1) {
            imgData.put("tiles", true);
            imgData.put("tile_size", getImageDataTileSize(pixelBuffer));
            imgData.put("levels", resLvlCount);
        } else {
            imgData.put("tiles", false);
        }

        imgData.put("interpolate", interpolate);

        imgData.put("size", getImageDataSize(pixelBuffer));

        imgData.put("pixel_size", getImageDataPixelSize(pixels));

        imgData.put("init_zoom", initZoom);

        if (resLvlCount > 1) {
            imgData.put("zoomLevelScaling",
                    getImageDataZoomLevelScaling(renderer));
        }

        imgData.put("pixel_range", getImageDataPixelRange(pixelBuffer));

        imgData.put("channels", getImageDataChannels(pixels, renderer));

        imgData.put("split_channel", getImageDataSplitChannel(pixels));

        imgData.put("rdefs", getImageDataRdef(rdef));

        return imgData;
    }

    private JsonObject getImageDataMeta(Image image, Pixels pixels,
            Event creationEvent, Experimenter owner) {
        JsonObject meta = new JsonObject();
        meta.put("imageName", unwrap(image.getName()));
        if (image.getDescription() == null) {
            meta.put("imageDescription", "");
        } else {
            meta.put("imageDescription", unwrap(image.getDescription()));
        }
        if (image.getAcquisitionDate() != null) {
            meta.put("imageTimestamp",
                    image.getAcquisitionDate().getValue() / 1000);
        } else {
            meta.put("imageTimestamp",
                    creationEvent.getTime().getValue() / 1000);
        }
        if (owner.getFirstName() != null && owner.getLastName() != null) {
            meta.put("imageAuthor", String.valueOf(unwrap(owner.getFirstName()) + " "
                    + String.valueOf(unwrap(owner.getLastName()))));
        }
        meta.put("imageAuthorId", owner.getId().getValue());
        List<Dataset> datasets = image.linkedDatasetList();
        if (datasets != null && datasets.size() > 1) {
            meta.put("datasetName", "Multiple");
            Set<Long> projectIds = new HashSet<Long>();
            for (Dataset ds : datasets) {
                List<Project> projects = ds.linkedProjectList();
                if (projects.size() > 1) {
                    meta.put("projectName", "Multiple");
                    break;
                } else if (projects.size() == 1) {
                    if (!projectIds.isEmpty() && !projectIds
                            .contains(projects.get(0).getId().getValue())) {
                        meta.put("projectName", "Multiple");
                        break;
                    } else {
                        projectIds.add(projects.get(0).getId().getValue());
                    }
                }
            }
            if (!meta.containsKey("projectName")) {
                Project project = datasets.get(0).linkedProjectList().get(0);
                meta.put("projectName", unwrap(project.getName()));
                meta.put("projectId", project.getId().getValue());
                meta.put("projectDescription",
                        unwrap(project.getDescription()));
            }
        } else if (datasets.size() == 1) {
            Dataset ds = datasets.get(0);
            meta.put("datasetName", unwrap(ds.getName()));
            meta.put("datasetId", ds.getId().getValue());
            meta.put("datasetDescription", unwrap(ds.getDescription()));
            List<Project> projects = ds.linkedProjectList();
            if (projects.size() > 1) {
                meta.put("projectName", "Multiple");
            } else if (projects.size() == 1) {
                Project project = projects.get(0);
                meta.put("projectName", unwrap(project.getName()));
                meta.put("projectId", project.getId().getValue());
                meta.put("projectDescription",
                        unwrap(project.getDescription()));
            }
        } else if (datasets.size() == 0) {
            //Dataset and Project names are Multiple to match omero-web
            meta.put("datasetName", "Multiple");
            meta.putNull("datasetId");
            meta.put("datasetDescription", "");
            meta.put("projectName", "Multiple");
            meta.putNull("projectId");
            meta.put("projectDescription", "");
        }
        if (image.sizeOfWellSamples() > 0) {
            WellSampleI wellSample = (WellSampleI) image.copyWellSamples().get(0);
            meta.put("wellSampleId", wellSample.getId().getValue());
            meta.put("wellId", wellSample.getWell().getId().getValue());
            if (image.sizeOfWellSamples() > 1) {
                meta.put("wellSampleId", "");
            }
        } else {
            meta.put("wellSampleId", "");
            meta.put("wellId", "");
        }
        meta.put("imageId", image.getId().getValue());
        meta.put("pixelsType",
                unwrap(pixels.getPixelsType().getValue()));
        return meta;
    }

    private JsonObject getImageDataPerms(Permissions permissions) {
        JsonObject perms = new JsonObject();
        perms.put("canAnnotate", permissions.canAnnotate());
        perms.put("canEdit", permissions.canEdit());
        perms.put("canLink", permissions.canLink());
        perms.put("canDelete", permissions.canDelete());
        return perms;
    }

    private JsonObject getImageDataTileSize(PixelBuffer pixelBuffer) {
        JsonObject tileSize = new JsonObject();
        tileSize.put("width", pixelBuffer.getTileSize().width);
        tileSize.put("height", pixelBuffer.getTileSize().height);
        return tileSize;
    }

    private JsonObject getImageDataSize(PixelBuffer pixelBuffer) {
        JsonObject size = new JsonObject();
        size.put("width", pixelBuffer.getSizeX());
        size.put("height", pixelBuffer.getSizeY());
        size.put("z", pixelBuffer.getSizeZ());
        size.put("c", pixelBuffer.getSizeC());
        size.put("t", pixelBuffer.getSizeT());
        return size;
    }

    private JsonObject getImageDataPixelSize(Pixels pixels) {
        JsonObject pixelSize = new JsonObject();
        try {
            pixelSize.put("x",
                    new LengthI(pixels.getPhysicalSizeX(), UNITS.MICROMETER)
                            .getValue());
            pixelSize.put("y",
                    new LengthI(pixels.getPhysicalSizeY(), UNITS.MICROMETER)
                            .getValue());
            if (pixels.getPhysicalSizeZ() != null) {
                pixelSize.put("z", new LengthI(pixels.getPhysicalSizeZ(),
                        UNITS.MICROMETER).getValue());
            }
        } catch (BigResult e) {
            log.error("BigResult error when converting pixel size", e);
        }
        return pixelSize;
    }

    private JsonObject getImageDataZoomLevelScaling(Renderer renderer) {
        JsonObject zoomLvlScaling = new JsonObject();
        List<List<Integer>> resDescs = renderer.getResolutionDescriptions();
        int maxXSize = resDescs.get(0).get(0);
        for (int i = 0; i < resDescs.size(); i++) {
            List<Integer> desc = resDescs.get(i);
            zoomLvlScaling.put(Integer.toString(i),
                    desc.get(0).doubleValue() / maxXSize);
        }
        return zoomLvlScaling;
    }

    private JsonArray getImageDataPixelRange(PixelBuffer pb) {
        long pmax = Math.round(Math.pow(2, 8 * pb.getByteWidth()));
        JsonArray pixelRange = new JsonArray();
        if (pb.isSigned()) {
            pixelRange.add(-1 * pmax / 2);
            pixelRange.add(pmax / 2 - 1);
        } else {
            pixelRange.add(0);
            pixelRange.add(pmax - 1);
        }
        return pixelRange;
    }

    private JsonArray getImageDataChannels(PixelsI pixels, Renderer renderer) {
        JsonArray channels = new JsonArray();
        int channelCount = pixels.sizeOfChannels();
        for (int i = 0; i < channelCount; i++) {
            Channel channel = pixels.getChannel(i);
            LogicalChannel logicalChannel = channel.getLogicalChannel();
            String label = null;
            logicalChannel.getName();
            if (logicalChannel.getName() != null
                    && logicalChannel.getName().getValue().length() > 0) {
                label = logicalChannel.getName().getValue();
            } else {
                if (logicalChannel.getEmissionWave() != null) {
                    label = logicalChannel.getEmissionWave().toString();
                } else {
                    label = Integer.toString(i);
                }
            }
            JsonObject ch = new JsonObject();
            if (logicalChannel.getEmissionWave() != null) {
                ch.put("emissionWave", logicalChannel.getEmissionWave().getValue());
            } else {
                ch.putNull("emissionWave");
            }
            ch.put("label", label);
            log.info(label);
            ch.put("inverted", isInverted(renderer, i));
            ch.put("reverseIntensity", isInverted(renderer, i));
            ChannelBinding cb = renderer.getChannelBindings()[i];
            ch.put("color", getColorString(cb));
            ch.put("family", cb.getFamily().getValue());
            ch.put("coefficient", cb.getCoefficient());
            ch.put("active", cb.getActive());
            StatsInfo statsInfo = channel.getStatsInfo();
            JsonObject window = new JsonObject();
            if (statsInfo != null) {
                window.put("min", statsInfo.getGlobalMin().getValue());
                window.put("max", statsInfo.getGlobalMax().getValue());
            } else {
                window.put("min", renderer.getPixelsTypeLowerBound(i));
                window.put("max", renderer.getPixelsTypeUpperBound(i));
            }
            window.put("start", cb.getInputStart());
            window.put("end", cb.getInputEnd());
            ch.put("window", window);

            channels.add(ch);
        }
        return channels;
    }

    private JsonObject getImageDataSplitChannel(Pixels pixels) {
        JsonObject splitChannel = new JsonObject();
        int c = pixels.sizeOfChannels();
        JsonObject g = new JsonObject();
        // Greyscale, no channel overlayed image
        double x = Math.sqrt(c);
        long y = Math.round(x);
        long longX = 0;
        if (x > y) {
            longX = y + 1;
        } else {
            longX = y;
        }
        int border = 2;
        g.put("width",
                pixels.getSizeX().getValue() * longX + border * (longX + 1));
        g.put("height", pixels.getSizeY().getValue() * y + border * (y + 1));
        g.put("border", border);
        g.put("gridx", longX);
        g.put("gridy", y);
        splitChannel.put("g", g);
        JsonObject clr = new JsonObject();
        // Color, one extra image with all channels overlayed
        c += 1;
        x = Math.sqrt(c);
        y = Math.round(x);
        if (x > y) {
            longX = y + 1;
        } else {
            longX = y;
        }
        clr.put("width",
                pixels.getSizeX().getValue() * longX + border * (longX + 1));
        clr.put("height", pixels.getSizeY().getValue() * y + border * (y + 1));
        clr.put("border", border);
        clr.put("gridx", longX);
        clr.put("gridy", y);
        splitChannel.put("c", clr);
        return splitChannel;
    }

    private JsonObject getImageDataRdef(RenderingDef rdef) {
        JsonObject rdefObj = new JsonObject();
        if (rdef.getModel().getValue().toLowerCase().equals("greyscale")) {
            rdefObj.put("model", "greyscale");
        } else {
            rdefObj.put("model", "color");
        }
        // "projection" and "invertAxis" are always going to be
        // "normal" and false in standard OMERO installs.
        rdefObj.put("projection", "normal");
        rdefObj.put("invertAxis", false);
        rdefObj.put("defaultZ", rdef.getDefaultZ());
        rdefObj.put("defaultT", rdef.getDefaultT());
        return rdefObj;
    }

    private boolean isInverted(Renderer renderer, int channel) {
        CodomainChain chain = renderer.getCodomainChain(channel);
        if (chain == null) {
            return false;
        }
        List<CodomainMapContext> mapContexts = chain.getContexts();
        for (CodomainMapContext cmctx : mapContexts) {
            if (cmctx instanceof ReverseIntensityContext) {
                return true;
            }
        }
        return false;
    }

    public static String getColorString(ChannelBinding cb) {
        log.info("R:" + Integer.toString(cb.getRed()));
        log.info("G:" + Integer.toString(cb.getGreen()));
        log.info("B:" + Integer.toString(cb.getBlue()));
        StringBuilder colorBuilder = new StringBuilder();
        if (cb.getRed() < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(cb.getRed())
                .toUpperCase());
        if (cb.getGreen() < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(cb.getGreen())
                .toUpperCase());
        if (cb.getBlue() < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(cb.getBlue())
                .toUpperCase());
        return colorBuilder.toString();
    }

    protected ImageI queryImageData(IQueryPrx iQuery, Long imageId)
            throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("query_image_data");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_id", imageId.toString());
            ParametersI params = new ParametersI();
            params.addId(imageId);
            ImageI image = (ImageI) iQuery
                    .findByQuery("select i from Image as i "
                            + " join fetch i.pixels as p"
                            + " left outer JOIN FETCH i.datasetLinks as links "
                            + " left outer join fetch links.parent as dataset "
                            + " left outer join fetch dataset.projectLinks as plinks "
                            + " left outer join fetch plinks.parent as project "
                            + " left outer join fetch i.objectiveSettings as os "
                            + " left outer join fetch os.objective as objective "
                            + " join fetch i.details.owner as owner "
                            + " join fetch i.details.creationEvent "
                            + " left outer join fetch i.wellSamples as ws"
                            + " left outer join fetch ws.well"
                            + " join fetch p.pixelsType "
                            + " join fetch p.channels as c "
                            + " join fetch c.logicalChannel as lc "
                            + " left outer join fetch c.statsInfo "
                            + " left outer join fetch lc.photometricInterpretation "
                            + " left outer join fetch lc.illumination "
                            + " left outer join fetch lc.mode "
                            + " left outer join fetch lc.contrastMethod "
                            + " where i.id=:id", params, ctx);
            return image;
        } finally {
            span.finish();
        }
    }

    /**
     * Returns a pixel buffer for a given set of pixels.
     *
     * @param pixels pixels metadata
     * @return See above.
     * @throws ApiUsageException
     * @see PixelsService#getPixelBuffer(Pixels)
     */
    private PixelBuffer getPixelBuffer(Pixels pixels)
            throws ApiUsageException {
        Tracer tracer = Tracing.currentTracer();
        ScopedSpan span = tracer.startScopedSpan("get_pixel_buffer");
        try {
            span.tag("omero.pixels_id",
                    Long.toString(pixels.getId().getValue()));
            return pixelsService.getPixelBuffer(
                    (ome.model.core.Pixels) mapper.reverse(pixels), false);
        } catch (ApiUsageException e) {
            span.error(e);
            throw e;
        } finally {
            span.finish();
        }
    }

    /**
     * Retrieves rendering settings either from the user or image owner
     * corresponding to any of the specified pixels sets.
     *
     * @param client    OMERO client to use for querying.
     * @param userId    The current user ID.
     * @param pixelsIds The pixels set identifiers.
     * @return See above.
     */
    protected List<RenderingDef> retrieveRenderingDefs(omero.client client,
            final long userId, final List<Long> pixelsIds) throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_rendering_defs");
        // Ask for rendering settings for the current user or the image owner
        String q = PixelsImpl.RENDERING_DEF_QUERY_PREFIX
                + "rdef.pixels.id in (:ids) " + "and ("
                + "  rdef.details.owner.id = rdef.pixels.details.owner.id"
                + "    or rdef.details.owner.id = :userId" + ")";
        try {
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            ParametersI params = new ParametersI();
            params.addIds(pixelsIds);
            params.add("userId", omero.rtypes.rlong(userId));
            return (List<RenderingDef>) mapper
                    .reverse(iQuery.findAllByQuery(q, params, ctx));
        } catch (Exception e) {
            span.error(e);
            return null;
        } finally {
            span.finish();
        }
    }

    /**
     * Selects the correct rendering settings either from the user (preferred) or
     * image owner corresponding to the specified pixels set.
     *
     * @param renderingDefs A list of rendering settings to select from.
     * @param pixelsId      The identifier of the pixels.
     * @return See above.
     */
    protected RenderingDef selectRenderingDef(List<RenderingDef> renderingDefs,
            final long userId, final long pixelsId) throws ServerError {
        RenderingDef userRenderingDef = renderingDefs.stream()
                .filter(v -> v.getPixels().getId() == pixelsId)
                .filter(v -> v.getDetails().getOwner().getId() == userId)
                .findFirst().orElse(null);
        if (userRenderingDef != null) {
            return userRenderingDef;
        }
        // Otherwise pick the first (from the owner) if available
        return renderingDefs.stream()
                .filter(v -> v.getPixels().getId() == pixelsId).findFirst()
                .orElse(null);
    }
}
