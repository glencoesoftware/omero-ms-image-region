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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.PixelsService;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import ome.logic.PixelsImpl;
import ome.model.units.BigResult;
import omero.model.Channel;
import omero.model.ChannelBinding;
import omero.model.CodomainMapContext;
import omero.model.LogicalChannel;
import omero.model.Dataset;
import omero.model.Project;
import omero.model.RenderingDef;
import omero.model.ReverseIntensityContext;
import omero.model.Image;
import omero.model.Length;
import omero.model.Permissions;
import omero.model.Pixels;
import omero.model.PixelsType;
import omero.model.LengthI;
import omero.model.StatsInfo;
import omero.model.Event;
import omero.model.Experimenter;
import omero.model.IObject;
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.WellSampleI;
import omero.model.enums.UnitsLength;
import omero.sys.ParametersI;
import omero.util.IceMapper;
import omeis.providers.re.metadata.StatsFactory;

import static omero.rtypes.unwrap;
import static omero.rtypes.rlong;

public class ImageDataRequestHandler {

    private static final org.slf4j.Logger log = LoggerFactory
            .getLogger(ImageDataRequestHandler.class);

    /** Image Data Context **/
    private ImageDataCtx imageDataCtx;

    /** OMERO server pixels service. */
    private PixelsService pixelsService;

    /** Initial Zoom level from server settings **/
    private int initZoom;

    /** Interpolation server setting **/
    private boolean interpolate;

    /**
     * Constructor
     * @param imageDataCtx Image Data Context
     * @param pixelsService OMERO server pixels service.
     * @param initZoom Initial Zoom level from server settings
     * @param interpolate Interpolation server setting
     */
    public ImageDataRequestHandler(ImageDataCtx imageDataCtx,
            PixelsService pixelsService,
            int initZoom, boolean interpolate) {
        this.imageDataCtx = imageDataCtx;
        this.pixelsService = pixelsService;
        this.initZoom = initZoom;
        this.interpolate = interpolate;
    }

    /**
     * Get the image data as a VertX JsonObject
     * @param client Omero client object
     * @return JsonObject with the image data/metadata
     */
    public JsonObject getImageData(omero.client client) {
        ServiceFactoryPrx sf = client.getSession();
        try {
            Long imageId = imageDataCtx.imageId;
            IQueryPrx iQuery = sf.getQueryService();
            Image image = queryImageData(iQuery, imageId);
            if (image == null) {
                return null;
            }
            Pixels pixels = image.getPrimaryPixels();
            List<Long> imageIds = new ArrayList<Long>();
            imageIds.add(imageId);
            long userId = sf.getAdminService().getEventContext().userId;
            try (PixelBuffer pixelBuffer = getPixelBuffer(pixels)) {
                List<Long> pixIds = new ArrayList<Long>();
                pixIds.add(pixels.getId().getValue());
                List<IObject> rdefs = retrieveRenderingDefs(client, userId,
                        pixIds);

                Map<String, String> pixCtx = new HashMap<String, String>();
                pixCtx.put("omero.group", "-1");
                return populateImageData(image, pixelBuffer, rdefs, userId);
            }
        } catch (Exception e) {
            log.error("Error getting image data", e);
        }
        return null;
    }

    /**
     * Takes populated Omero model objects and populates the data into
     * a JsonObject
     * @param image
     * @param pixelBuffer
     * @param rdefs
     * @param userId
     * @return
     * @throws ApiUsageException
     */
    public JsonObject populateImageData(
            Image image, PixelBuffer pixelBuffer, List<IObject> rdefs,
            long userId) throws ApiUsageException {
        Permissions permissions = image.getDetails().getPermissions();
        Pixels pixels = image.getPrimaryPixels();
        RenderingDef rdef = selectRenderingDef(
                rdefs, userId, pixels.getId().getValue());

        JsonObject imgData = new JsonObject();
        imgData.put("id", image.getId().getValue());
        JsonObject meta = getImageDataMeta(image);
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

        imgData.put("size", getImageDataSize(pixels));

        imgData.put("pixel_size", getImageDataPixelSize(pixels));

        imgData.put("init_zoom", initZoom);

        if (resLvlCount > 1) {
            imgData.put("zoomLevelScaling",
                    getImageDataZoomLevelScaling(pixelBuffer));
        }

        try {
            imgData.put("pixel_range", getImageDataPixelRange(pixels));

            imgData.put("channels", getImageDataChannels(pixels, rdef));

            imgData.put("split_channel", getImageDataSplitChannel(pixels));

            imgData.put("rdefs", getImageDataRdef(rdef));
        } catch (Exception e) {
            log.error("Error handling pixel data or rendering definition", e);
            // "Error" data in accordance with omeroweb.webgateway.marshal
            imgData.put("pixel_range", new JsonArray(Arrays.asList(0, 0)));
            imgData.put("channels", new JsonArray());
            imgData.put("split_channel", new JsonArray());
            JsonObject rdefsJson = new JsonObject();
            rdefsJson.put("model", "color");
            rdefsJson.put("projection", "normal");
            rdefsJson.put("defaultZ", 0);
            rdefsJson.put("defaultT", 0);
            rdefsJson.put("invertAxis", false);
            imgData.put("rdefs", rdefsJson);
        }

        return imgData;
    }

    /**
     * Populates the "metadata" in the image data JsonObject
     * @param image
     * @return JsonObject with metadata
     */
    private JsonObject getImageDataMeta(Image image) {
        Event creationEvent = image.getDetails().getCreationEvent();
        Experimenter owner = image.getDetails().getOwner();
        PixelsType pixelsType = image.getPrimaryPixels().getPixelsType();
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
        // Unless there is exactly 1 project or dataset associated
        // with the given image, the name will be "Multiple",
        // the id will be null and the description will be ""
        // to match OMERO.web behavior
        meta.put("datasetName", "Multiple");
        meta.putNull("datasetId");
        meta.put("datasetDescription", "");
        meta.put("projectName", "Multiple");
        meta.putNull("projectId");
        meta.put("projectDescription", "");
        List<Dataset> datasets = image.linkedDatasetList();
        if (datasets.size() > 1) {
            Set<Long> projectIds = new HashSet<Long>();
            for (Dataset ds : datasets) {
                List<Project> projects = ds.linkedProjectList();
                if (projects.size() == 1) {
                    if (!projectIds.isEmpty() && !projectIds
                            .contains(projects.get(0).getId().getValue())) {
                        meta.put("projectName", "Multiple");
                        meta.putNull("projectId");
                        meta.put("projectDescription", "");
                        break;
                    } else {
                        Project project = projects.get(0);
                        projectIds.add(project.getId().getValue());
                        // In case this is the only dataset with a project,
                        // set the properties here
                        meta.put("projectName", unwrap(project.getName()));
                        meta.put("projectId", project.getId().getValue());
                        meta.put("projectDescription", project.getDescription()
                                == null ? "" : unwrap(project.getDescription()));
                    }
                }
            }
        } else if (datasets.size() == 1) {
            Dataset ds = datasets.get(0);
            meta.put("datasetName", unwrap(ds.getName()));
            meta.put("datasetId", ds.getId().getValue());
            meta.put("datasetDescription", ds.getDescription() == null ? ""
                    : unwrap(ds.getDescription()));
            List<Project> projects = ds.linkedProjectList();
            if (projects.size() == 1) {
                Project project = projects.get(0);
                meta.put("projectName", unwrap(project.getName()));
                meta.put("projectId", project.getId().getValue());
                meta.put("projectDescription", project.getDescription() == null
                        ? "" : unwrap(project.getDescription()));
            }
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
        meta.put("pixelsType", unwrap(pixelsType.getValue()));
        return meta;
    }

    /**
     * Populates the "perms" element of the image data JsonObject
     * @param permissions
     * @return JsonObject with permissions data
     */
    private JsonObject getImageDataPerms(Permissions permissions) {
        JsonObject perms = new JsonObject();
        perms.put("canAnnotate", permissions.canAnnotate());
        perms.put("canEdit", permissions.canEdit());
        perms.put("canLink", permissions.canLink());
        perms.put("canDelete", permissions.canDelete());
        return perms;
    }

    /**
     * Populates the tile_size image data
     * @param pixelBuffer
     * @return the tile size JsonObject
     */
    private JsonObject getImageDataTileSize(PixelBuffer pixelBuffer) {
        JsonObject tileSize = new JsonObject();
        tileSize.put("width", pixelBuffer.getTileSize().width);
        tileSize.put("height", pixelBuffer.getTileSize().height);
        return tileSize;
    }

    /**
     * Populates the size image data
     * @param pixels
     * @return The size JsonObject
     */
    private JsonObject getImageDataSize(Pixels pixels) {
        JsonObject size = new JsonObject();
        size.put("width", unwrap(pixels.getSizeX()));
        size.put("height", unwrap(pixels.getSizeY()));
        size.put("z", unwrap(pixels.getSizeZ()));
        size.put("c", unwrap(pixels.getSizeC()));
        size.put("t", unwrap(pixels.getSizeT()));
        return size;
    }

    /**
     * Converts length to microns or <code>null</code> if the length is null
     * or the source unit cannot be converted.
     * @param length
     * @return See above.
     */
    private Double asMicrons(Length length) {
        if (length == null) {
            return null;
        }

        UnitsLength unit = length.getUnit();
        if (unit.equals(UnitsLength.PIXEL)
                || unit.equals(UnitsLength.REFERENCEFRAME)) {
            log.warn("Cannot convert {} to microns", length);
            return null;
        }
        try {
            return new LengthI(length, UnitsLength.MICROMETER).getValue();
        } catch (BigResult e) {
            log.error("Error while converting pixel size to microns", e);
            return null;
        }
    }

    /**
     * Populate the pixel size image data
     * @param pixels
     * @return The pixel size JsonObject
     */
    private JsonObject getImageDataPixelSize(Pixels pixels) {
        JsonObject pixelSize = new JsonObject();
        Double physicalSizeX = asMicrons(pixels.getPhysicalSizeX());
        pixelSize.put("x", physicalSizeX);
        Double physicalSizeY = asMicrons(pixels.getPhysicalSizeY());
        pixelSize.put("y", physicalSizeY);
        Double physicalSizeZ = asMicrons(pixels.getPhysicalSizeZ());
        pixelSize.put("z", physicalSizeZ);
        return pixelSize;
    }

    /**
     * Populate zoom level scaling image data
     * @param pixelBuffer
     * @return the zoom level scaling image data
     */
    private JsonObject getImageDataZoomLevelScaling(PixelBuffer pixelBuffer) {
        JsonObject zoomLvlScaling = new JsonObject();
        List<List<Integer>> resDescs = pixelBuffer.getResolutionDescriptions();
        int maxXSize = resDescs.get(0).get(0);
        for (int i = 0; i < resDescs.size(); i++) {
            List<Integer> desc = resDescs.get(i);
            zoomLvlScaling.put(Integer.toString(i),
                    desc.get(0).doubleValue() / maxXSize);
        }
        return zoomLvlScaling;
    }

    /**
     * Retrieve the pixels type range for a given pixels set
     * @param pixels
     * @return See above.
     * @throws ApiUsageException
     */
    private double[] getPixelsRange(Pixels pixels) throws ApiUsageException {
        StatsFactory statsFactory = new StatsFactory();
        return statsFactory.initPixelsRange(
                (ome.model.core.Pixels) new IceMapper().reverse(pixels));
    }

    /**
     * Populate pixel range image data
     * NOTE: Unlike omero-web, <b>DOES</b> handle floats correctly.
     * @param pixels
     * @return The pixel range JsonObject
     * @throws ApiUsageException
     */
    private JsonArray getImageDataPixelRange(Pixels pixels)
            throws ApiUsageException {
        double[] minMax = getPixelsRange(pixels);
        JsonArray pixelRange = new JsonArray();
        pixelRange.add(Math.round(minMax[0]));
        pixelRange.add(Math.round(minMax[1]));
        return pixelRange;
    }

    /**
     * Populate channel image data
     * @param pixels
     * @param rdef
     * @return JsonArray of channels image data
     * @throws ApiUsageException
     */
    private JsonArray getImageDataChannels(Pixels pixels, RenderingDef rdef)
            throws ApiUsageException {
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
            ch.put("inverted", isInverted(rdef, i));
            ch.put("reverseIntensity", isInverted(rdef, i));
            ChannelBinding cb = rdef.getChannelBinding(i);
            ch.put("color", getColorString(cb));
            ch.put("family", cb.getFamily().getValue().getValue());
            ch.put("coefficient", cb.getCoefficient().getValue());
            ch.put("active", cb.getActive().getValue());
            StatsInfo statsInfo = channel.getStatsInfo();
            JsonObject window = new JsonObject();
            if (statsInfo != null) {
                window.put("min", statsInfo.getGlobalMin().getValue());
                window.put("max", statsInfo.getGlobalMax().getValue());
            } else {
                double[] minMax = getPixelsRange(pixels);
                window.put("min", minMax[0]);
                window.put("max", minMax[1]);
            }
            window.put("start", cb.getInputStart().getValue());
            window.put("end", cb.getInputEnd().getValue());
            ch.put("window", window);

            channels.add(ch);
        }
        return channels;
    }

    /**
     * Populate split channel image data
     * @param pixels
     * @return The split channel JsonObject
     */
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
        String model = (String) unwrap(rdef.getModel().getValue());
        if (model.toLowerCase().equals("greyscale")) {
            rdefObj.put("model", "greyscale");
        } else {
            rdefObj.put("model", "color");
        }
        // "projection" and "invertAxis" are always going to be
        // "normal" and false in standard OMERO installs.
        rdefObj.put("projection", "normal");
        rdefObj.put("invertAxis", false);
        rdefObj.put("defaultZ", rdef.getDefaultZ().getValue());
        rdefObj.put("defaultT", rdef.getDefaultT().getValue());
        return rdefObj;
    }

    /**
     * Whether the channel is inverted
     * @param renderer
     * @param channel
     * @return Whether the channel is inverted
     */
    private boolean isInverted(RenderingDef rdef, int channel) {
        ChannelBinding cb = rdef.getChannelBinding(channel);
        List<CodomainMapContext> mapContexts =
                cb.copySpatialDomainEnhancement();
        for (CodomainMapContext mapContext : mapContexts) {
            if (mapContext instanceof ReverseIntensityContext) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get a hex string representing the color from a ChannelBinding
     */
    /**
     * Get a hex string representing the color from a ChannelBinding
     * e.g. 00FF00 for green
     * @param cb ChannelBinding to get the color of
     * @return Hex string representing the color e.g. 00FF00 for green
     */
    public static String getColorString(ChannelBinding cb) {
        StringBuilder colorBuilder = new StringBuilder();
        int red = cb.getRed().getValue();
        int green = cb.getGreen().getValue();
        int blue = cb.getBlue().getValue();
        if (red < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(red)
                .toUpperCase());
        if (green < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(green)
                .toUpperCase());
        if (blue < 16) {
            colorBuilder.append("0");
        }
        colorBuilder.append(Integer.toHexString(blue)
                .toUpperCase());
        return colorBuilder.toString();
    }

    /**
     * Query the server to get all possible relevant data about the image
     * @param iQuery
     * @param imageId
     * @return ImageI object containing most image data
     * @throws ApiUsageException
     * @throws ServerError
     */
    protected Image queryImageData(IQueryPrx iQuery, Long imageId)
            throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("query_image_data");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_id", imageId.toString());
            ParametersI params = new ParametersI();
            params.addId(imageId);
            Image image = (Image) iQuery
                    .findByQuery("select i from Image as i"
                            + " left outer join fetch i.details.externalInfo "
                            + " join fetch i.pixels as p"
                            + " join fetch i.format"
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
            return pixelsService.getPixelBuffer((ome.model.core.Pixels)
                     new IceMapper().reverse(pixels), false);
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
    protected List<IObject> retrieveRenderingDefs(omero.client client,
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
            params.add("userId", rlong(userId));
            return iQuery.findAllByQuery(q, params, ctx);
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
     * @param userId Current user ID making the request.
     * @param pixelsId The identifier of the pixels.
     * @return See above.
     */
    protected RenderingDef selectRenderingDef(List<IObject> renderingDefs,
            final long userId, final long pixelsId) {
        RenderingDef userRenderingDef = renderingDefs.stream()
            .map(RenderingDef.class::cast)
            .filter(v -> v.getPixels().getId().getValue() == pixelsId)
            .filter(v -> v.getDetails().getOwner().getId().getValue() == userId)
            .findFirst()
            .orElse(null);
        if (userRenderingDef != null) {
            return userRenderingDef;
        }
        // Otherwise pick the first (from the owner) if available
        return renderingDefs.stream()
                .map(RenderingDef.class::cast)
                .filter(v -> v.getPixels().getId().getValue() == pixelsId)
                .findFirst()
                .orElse(null);
    }
}
