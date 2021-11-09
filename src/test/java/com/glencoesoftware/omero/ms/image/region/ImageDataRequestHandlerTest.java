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

import static org.mockito.Mockito.*;

import java.awt.Dimension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import ome.model.display.ChannelBinding;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import omero.model.PixelsTypeI;
import ome.model.enums.RenderingModel;
import ome.units.UNITS;
import omeis.providers.re.Renderer;
import omeis.providers.re.codomain.CodomainChain;
import omeis.providers.re.codomain.CodomainMapContext;
import omeis.providers.re.codomain.ReverseIntensityContext;
import omero.rtypes;
import omero.model.Channel;
import omero.model.ChannelI;
import omero.model.DatasetI;
import omero.model.DatasetImageLinkI;
import omero.model.EventI;
import omero.model.Experimenter;
import omero.model.ExperimenterI;
import omero.model.ImageI;
import omero.model.LengthI;
import omero.model.LogicalChannelI;
import omero.model.ObjectiveI;
import omero.model.ObjectiveSettingsI;
import omero.model.Permissions;
import omero.model.PermissionsI;
import omero.model.PixelsI;
import omero.model.ProjectDatasetLinkI;
import omero.model.ProjectI;
import omero.model.StatsInfoI;
import omero.model.WellI;
import omero.model.WellSample;
import omero.model.WellSampleI;
import omero.model.enums.UnitsLength;

import org.junit.Assert;
import org.junit.Before;

public class ImageDataRequestHandlerTest {

    ImageI image;
    PixelsI pixels;
    EventI creationEvent;
    Experimenter owner;
    Optional<WellSampleI> wellSample;
    Permissions permissions;
    PixelBuffer pixelBuffer;
    Renderer renderer;
    RenderingDef rdef;
    ChannelBinding[] cbs;

    public static String OWNER_FIRST_NAME = "firstname";
    public static String OWNER_LAST_NAME = "lastname";
    public static Long OWNER_ID = 123l;

    public static long IMAGE_ID = 1l;
    public static String IMAGE_NAME = "test name";
    public static String IMAGE_DESC = "test image description";
    public static long IMAGE_TIMESTAMP = 12345l;

    public static long DATASET_LINK_ID_1 = 10l;

    public static long DATASET_ID_1 = 100l;
    public static String DATASET_NAME_1 = "ds name 1";
    public static String DATASET_DESC_1 = "ds description 1";

    public static long PROJECT_DS_LINK_ID_1 = 1000l;

    public static long PROJECT_ID_1 = 10000l;
    public static String PROJECT_NAME_1 = "proj name 1";
    public static String PROJECT_DESC_1 = "proj description 1";

    public static long PIXELS_ID = 12l;
    public static String PIX_TYPE_STR = "uint8";

    public static boolean CAN_ANNOTATE = true;
    public static boolean CAN_DELETE = true;
    public static boolean CAN_EDIT = true;
    public static boolean CAN_LINK = true;

    public static boolean TILES_BOOL = true;
    public static Integer RES_LVL_COUNT = 5;
    public static double ZOOM_LVL_0 = 1;
    public static double ZOOM_LVL_1 = 0.5;
    public static double ZOOM_LVL_2 = 0.25;
    public static double ZOOM_LVL_3 = 0.125;
    public static double ZOOM_LVL_4 = 0.0625;

    public static Integer TILE_WIDTH = 256;
    public static Integer TILE_HEIGHT = 512;
    public static int PIXELS_SIZE_X = 512;
    public static int PIXELS_SIZE_Y = 1024;
    public static int PIXELS_SIZE_Z = 1;
    public static int PIXELS_SIZE_C = 3;
    public static int PIXELS_SIZE_T = 1;

    public static double PHYSICAL_SIZE_X = 1.0;
    public static double PHYSICAL_SIZE_Y = 2.0;

    public static int PIX_RANGE_START = 0;
    public static int PIX_RANGE_END = 255;

    public static boolean INTERPOLATE_BOOL = true;
    public static int INIT_ZOOM = 0;

    public static int BYTE_WIDTH = 1;

    public static long WELL_SAMPLE_ID = 2l;
    public static long WELL_ID = 20l;

    public static double NOMINAL_MAGNIFICATION = 123.456;


    public static boolean CH1_ACTIVE = true;
    public static double CH1_COEFFICIENT = 1.1;
    public static String CH1_COLOR = "FF0000";
    public static String CH1_FAMILY = "linear";
    public static boolean CH1_INVERTED = false;
    public static String CH1_LABEL = "channel name 1";
    public static double CH1_WINDOW_START = 0;
    public static double CH1_WINDOW_END = 30;
    public static double CH1_WINDOW_MIN = -10;
    public static double CH1_WINDOW_MAX = 10;

    public static boolean CH2_ACTIVE = true;
    public static double CH2_COEFFICIENT = 1.2;
    public static String CH2_COLOR = "00FF00";
    public static String CH2_FAMILY = "linear";
    public static boolean CH2_INVERTED = false;
    public static String CH2_LABEL = "channel name 2";
    public static double CH2_WINDOW_START = 0;
    public static double CH2_WINDOW_END = 31;
    public static double CH2_WINDOW_MIN = -20;
    public static double CH2_WINDOW_MAX = 20;


    public static boolean CH3_ACTIVE = true;
    public static double CH3_COEFFICIENT = 1.3;
    public static String CH3_COLOR = "0000FF";
    public static String CH3_FAMILY = "linear";
    public static boolean CH3_INVERTED = false;
    public static String CH3_LABEL = "channel name 3";
    public static double CH3_WINDOW_START = 0;
    public static double CH3_WINDOW_END = 32;
    public static double CH3_WINDOW_MIN = -30;
    public static double CH3_WINDOW_MAX = 30;

    public static int SPLIT_CH_C_BORDER = 2;
    public static int SPLIT_CH_C_GRIDX = 2;
    public static int SPLIT_CH_C_GRIDY = 2;
    public static int SPLIT_CH_C_HEIGHT = 2054;
    public static int SPLIT_CH_C_WIDTH = 1030;

    public static int SPLIT_CH_G_BORDER = 2;
    public static int SPLIT_CH_G_GRIDX = 2;
    public static int SPLIT_CH_G_GRIDY = 2;
    public static int SPLIT_CH_G_HEIGHT = 2054;
    public static int SPLIT_CH_G_WIDTH = 1030;

    public static int DEFAULT_T = 0;
    public static int DEFAULT_Z = 1;
    public static boolean INVERT_AXIS = false;
    public static String MODEL = "color";
    public static String PROJECTION = "normal";

    JsonObject imgData;

    public void setupStdJson() {
        imgData = new JsonObject();
        imgData.put("id", IMAGE_ID);
        imgData.put("meta", getImageDataMetaTest());
        imgData.put("nominalMagnification", NOMINAL_MAGNIFICATION);

        imgData.put("perms", getImageDataPermsTest());

        imgData.put("tiles", TILES_BOOL);
        imgData.put("tile_size", getImageDataTileSizeTest());
        imgData.put("levels", RES_LVL_COUNT);

        imgData.put("interpolate", INTERPOLATE_BOOL);

        imgData.put("size", getImageDataSizeTest());

        imgData.put("pixel_size", getImageDataPixelSizeTest());

        imgData.put("init_zoom", INIT_ZOOM);

        imgData.put("zoomLevelScaling",
                getImageDataZoomLevelScalingTest());

        imgData.put("pixel_range", getImageDataPixelRangeTest());

        imgData.put("channels", getImageDataChannelsTest());

        imgData.put("split_channel", getImageDataSplitChannelTest());

        imgData.put("rdefs", getImageDataRdefTest());
    }

    public JsonObject getImageDataMetaTest() {
        JsonObject meta = new JsonObject();
        meta.put("datasetDescription", DATASET_DESC_1);
        meta.put("datasetId", DATASET_ID_1);
        meta.put("datasetName", DATASET_NAME_1);
        meta.put("imageAuthor", OWNER_FIRST_NAME + " " + OWNER_LAST_NAME);
        meta.put("imageAuthorId", OWNER_ID);
        meta.put("imageDescription", IMAGE_DESC);
        meta.put("imageId", IMAGE_ID);
        meta.put("imageName", IMAGE_NAME);
        meta.put("imageTimestamp", IMAGE_TIMESTAMP);
        meta.put("pixelsType", PIX_TYPE_STR);
        meta.put("projectDescription", PROJECT_DESC_1);
        meta.put("projectId", PROJECT_ID_1);
        meta.put("projectName", PROJECT_NAME_1);
        meta.put("wellId", WELL_ID);
        meta.put("wellSampleId", WELL_SAMPLE_ID);
        return meta;
    }

    public JsonObject getImageDataPermsTest() {
        JsonObject perms = new JsonObject();
        perms.put("canAnnotate", CAN_ANNOTATE);
        perms.put("canDelete", CAN_DELETE);
        perms.put("canEdit", CAN_EDIT);
        perms.put("canLink", CAN_LINK);
        return perms;
    }

    public JsonObject getImageDataTileSizeTest() {
        JsonObject tileSize = new JsonObject();
        tileSize.put("height", TILE_HEIGHT);
        tileSize.put("width", TILE_WIDTH);
        return tileSize;
    }

    public JsonObject getImageDataSizeTest() {
        JsonObject size = new JsonObject();
        size.put("c", PIXELS_SIZE_C);
        size.put("height", PIXELS_SIZE_Y);
        size.put("t", PIXELS_SIZE_T);
        size.put("width", PIXELS_SIZE_X);
        size.put("z", PIXELS_SIZE_Z);
        return size;
    }

    public JsonObject getImageDataPixelSizeTest() {
        JsonObject pixelSize = new JsonObject();
        pixelSize.put("x", PHYSICAL_SIZE_X);
        pixelSize.put("y", PHYSICAL_SIZE_Y);
        pixelSize.putNull("z");
        return pixelSize;
    }

    public JsonObject getImageDataZoomLevelScalingTest() {
        JsonObject zoomLvlScaling = new JsonObject();
        zoomLvlScaling.put("0", ZOOM_LVL_0);
        zoomLvlScaling.put("1", ZOOM_LVL_1);
        zoomLvlScaling.put("2", ZOOM_LVL_2);
        zoomLvlScaling.put("3", ZOOM_LVL_3);
        zoomLvlScaling.put("4", ZOOM_LVL_4);
        return zoomLvlScaling;
    }

    public JsonArray getImageDataPixelRangeTest() {
        JsonArray pixelRange = new JsonArray();
        pixelRange.add(PIX_RANGE_START);
        pixelRange.add(PIX_RANGE_END);
        return pixelRange;
    }

    public JsonArray getImageDataChannelsTest() {
        JsonArray channels = new JsonArray();
        JsonObject ch1 = new JsonObject();
        ch1.put("active", CH1_ACTIVE);
        ch1.put("coefficient", CH1_COEFFICIENT);
        ch1.put("color", CH1_COLOR);
        ch1.put("family", CH1_FAMILY);
        ch1.put("inverted", CH1_INVERTED);
        ch1.put("reverseIntensity", CH1_INVERTED);
        ch1.put("label", CH1_LABEL);
        ch1.putNull("emissionWave");
        JsonObject ch1Window = new JsonObject();
        ch1Window.put("start", CH1_WINDOW_START);
        ch1Window.put("end", CH1_WINDOW_END);
        ch1Window.put("min", CH1_WINDOW_MIN);
        ch1Window.put("max", CH1_WINDOW_MAX);
        ch1.put("window", ch1Window);

        JsonObject ch2 = new JsonObject();
        ch2.put("active", CH2_ACTIVE);
        ch2.put("coefficient", CH2_COEFFICIENT);
        ch2.put("color", CH2_COLOR);
        ch2.put("family", CH2_FAMILY);
        ch2.put("inverted", CH2_INVERTED);
        ch2.put("reverseIntensity", CH2_INVERTED);
        ch2.put("label", CH2_LABEL);
        ch2.putNull("emissionWave");
        JsonObject ch2Window = new JsonObject();
        ch2Window.put("start", CH2_WINDOW_START);
        ch2Window.put("end", CH2_WINDOW_END);
        ch2Window.put("min", CH2_WINDOW_MIN);
        ch2Window.put("max", CH2_WINDOW_MAX);
        ch2.put("window", ch2Window);

        JsonObject ch3 = new JsonObject();
        ch3.put("active", CH3_ACTIVE);
        ch3.put("coefficient", CH3_COEFFICIENT);
        ch3.put("color", CH3_COLOR);
        ch3.put("family", CH3_FAMILY);
        ch3.put("inverted", CH3_INVERTED);
        ch3.put("reverseIntensity", CH3_INVERTED);
        ch3.put("label", CH3_LABEL);
        ch3.putNull("emissionWave");
        JsonObject ch3Window = new JsonObject();
        ch3Window.put("start", CH3_WINDOW_START);
        ch3Window.put("end", CH3_WINDOW_END);
        ch3Window.put("min", CH3_WINDOW_MIN);
        ch3Window.put("max", CH3_WINDOW_MAX);
        ch3.put("window", ch3Window);

        channels.add(ch1);
        channels.add(ch2);
        channels.add(ch3);

        return channels;
    }

    public JsonObject getImageDataSplitChannelTest() {
        JsonObject splitChannel = new JsonObject();
        JsonObject c = new JsonObject();
        c.put("border", SPLIT_CH_C_BORDER);
        c.put("gridx", SPLIT_CH_C_GRIDX);
        c.put("gridy", SPLIT_CH_C_GRIDY);
        c.put("height", SPLIT_CH_C_HEIGHT);
        c.put("width", SPLIT_CH_C_WIDTH);

        JsonObject g = new JsonObject();
        g.put("border", SPLIT_CH_G_BORDER);
        g.put("gridx", SPLIT_CH_G_GRIDX);
        g.put("gridy", SPLIT_CH_G_GRIDY);
        g.put("height", SPLIT_CH_G_HEIGHT);
        g.put("width", SPLIT_CH_G_WIDTH);

        splitChannel.put("c", c);
        splitChannel.put("g", g);
        return splitChannel;
    }

    public JsonObject getImageDataRdefTest() {
        JsonObject rdef = new JsonObject();
        rdef.put("defaultT", DEFAULT_T);
        rdef.put("defaultZ", DEFAULT_Z);
        rdef.put("invertAxis", INVERT_AXIS);
        rdef.put("model", MODEL);
        rdef.put("projection", PROJECTION);
        return rdef;
    }

    @Before
    public void setup() throws IOException {
        //imgData = new JsonObject(new String(Files.readAllBytes(Paths.get("./testImageData.json")), StandardCharsets.UTF_8));
        setupStdJson();

        creationEvent = new EventI();
        creationEvent.setTime(rtypes.rtime(IMAGE_TIMESTAMP*1000));
        owner = new ExperimenterI();
        owner.setFirstName(rtypes.rstring(OWNER_FIRST_NAME));
        owner.setLastName(rtypes.rstring(OWNER_LAST_NAME));
        owner.setId(rtypes.rlong(OWNER_ID));

        image = spy(ImageI.class);
        image.setId(rtypes.rlong(IMAGE_ID));
        image.setName(rtypes.rstring(IMAGE_NAME));
        image.setDescription(rtypes.rstring(IMAGE_DESC));
        DatasetImageLinkI dslink1 = new DatasetImageLinkI(DATASET_LINK_ID_1,
                true);
        DatasetI ds1 = new DatasetI(DATASET_ID_1, true);
        ds1.setName(rtypes.rstring(DATASET_NAME_1));
        ds1.setDescription(rtypes.rstring(DATASET_DESC_1));
        dslink1.setParent(ds1);
        ProjectDatasetLinkI projLink1 = new ProjectDatasetLinkI(
                PROJECT_DS_LINK_ID_1, true);
        ProjectI proj_1 = new ProjectI(PROJECT_ID_1, true);
        proj_1.setName(rtypes.rstring(PROJECT_NAME_1));
        proj_1.setDescription(rtypes.rstring(PROJECT_DESC_1));
        projLink1.setParent(proj_1);
        image.addDatasetImageLink(dslink1);
        ds1.addProjectDatasetLink(projLink1);

        pixels = new PixelsI(PIXELS_ID, true);
        PixelsTypeI pixType = new PixelsTypeI();
        pixType.setValue(rtypes.rstring(PIX_TYPE_STR));
        pixels.setPixelsType(pixType);
        pixels.setPhysicalSizeX(new LengthI(PHYSICAL_SIZE_X, UnitsLength.MICROMETER));
        pixels.setPhysicalSizeY(new LengthI(PHYSICAL_SIZE_Y, UnitsLength.MICROMETER));
        pixels.setSizeX(rtypes.rint(PIXELS_SIZE_X));
        pixels.setSizeY(rtypes.rint(PIXELS_SIZE_Y));

        ChannelI channel1 = new ChannelI();
        channel1.setRed(rtypes.rint(255));
        channel1.setGreen(rtypes.rint(0));
        channel1.setBlue(rtypes.rint(0));
        LogicalChannelI logCh1 = new LogicalChannelI();
        logCh1.setName(rtypes.rstring(CH1_LABEL));
        channel1.setLogicalChannel(logCh1);
        StatsInfoI statsInfo1 = new StatsInfoI();
        statsInfo1.setGlobalMin(rtypes.rdouble(CH1_WINDOW_MIN));
        statsInfo1.setGlobalMax(rtypes.rdouble(CH1_WINDOW_MAX));
        channel1.setStatsInfo(statsInfo1);

        ChannelI channel2 = new ChannelI();
        channel2.setRed(rtypes.rint(0));
        channel2.setGreen(rtypes.rint(255));
        channel2.setBlue(rtypes.rint(0));
        LogicalChannelI logCh2 = new LogicalChannelI();
        logCh2.setName(rtypes.rstring(CH2_LABEL));
        channel2.setLogicalChannel(logCh2);
        StatsInfoI statsInfo2 = new StatsInfoI();
        statsInfo2.setGlobalMin(rtypes.rdouble(CH2_WINDOW_MIN));
        statsInfo2.setGlobalMax(rtypes.rdouble(CH2_WINDOW_MAX));
        channel2.setStatsInfo(statsInfo2);

        ChannelI channel3 = new ChannelI();
        channel3.setRed(rtypes.rint(0));
        channel3.setGreen(rtypes.rint(0));
        channel3.setBlue(rtypes.rint(255));
        LogicalChannelI logCh3 = new LogicalChannelI();
        logCh3.setName(rtypes.rstring(CH3_LABEL));
        channel3.setLogicalChannel(logCh3);
        StatsInfoI statsInfo3 = new StatsInfoI();
        statsInfo3.setGlobalMin(rtypes.rdouble(CH3_WINDOW_MIN));
        statsInfo3.setGlobalMax(rtypes.rdouble(CH3_WINDOW_MAX));
        channel3.setStatsInfo(statsInfo3);

        pixels.addChannel(channel1);
        pixels.addChannel(channel2);
        pixels.addChannel(channel3);

        permissions = new PermissionsI("rwrwrw");

        wellSample = Optional.empty();

        pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getResolutionLevels()).thenReturn(RES_LVL_COUNT);
        when(pixelBuffer.getTileSize())
                .thenReturn(new Dimension(TILE_WIDTH, TILE_HEIGHT));
        when(pixelBuffer.getSizeX()).thenReturn(PIXELS_SIZE_X);
        when(pixelBuffer.getSizeY()).thenReturn(PIXELS_SIZE_Y);
        when(pixelBuffer.getSizeZ()).thenReturn(PIXELS_SIZE_Z);
        when(pixelBuffer.getSizeC()).thenReturn(PIXELS_SIZE_C);
        when(pixelBuffer.getSizeT()).thenReturn(PIXELS_SIZE_T);
        when(pixelBuffer.getByteWidth()).thenReturn(BYTE_WIDTH);
        when(pixelBuffer.isSigned()).thenReturn(false);

        renderer = mock(Renderer.class);
        List<List<Integer>> resLvlDescs = new ArrayList<List<Integer>>();
        resLvlDescs.add(Arrays.asList(PIXELS_SIZE_X, PIXELS_SIZE_Y));
        resLvlDescs.add(Arrays.asList((int) Math.round(
                Math.floor(PIXELS_SIZE_X * ZOOM_LVL_1)),
                (int) Math.round(Math.floor(PIXELS_SIZE_Y * ZOOM_LVL_1))));
        resLvlDescs.add(Arrays.asList((int) Math.round(
                Math.floor(PIXELS_SIZE_X * ZOOM_LVL_2)),
                (int) Math.round(Math.floor(PIXELS_SIZE_Y * ZOOM_LVL_2))));
        resLvlDescs.add(Arrays.asList((int) Math.round(
                Math.floor(PIXELS_SIZE_X * ZOOM_LVL_3)),
                (int) Math.round(Math.floor(PIXELS_SIZE_Y * ZOOM_LVL_3))));
        resLvlDescs.add(Arrays.asList((int) Math.round(
                Math.floor(PIXELS_SIZE_X * ZOOM_LVL_4)),
                (int) Math.round(Math.floor(PIXELS_SIZE_Y * ZOOM_LVL_4))));
        when(renderer.getResolutionDescriptions()).thenReturn(resLvlDescs);

        ChannelBinding cb1 = new ChannelBinding();
        cb1.setFamily(new Family(Family.VALUE_LINEAR));
        cb1.setCoefficient(CH1_COEFFICIENT);
        cb1.setActive(CH1_ACTIVE);
        cb1.setInputStart(CH1_WINDOW_START);
        cb1.setInputEnd(CH1_WINDOW_END);
        cb1.setRed(255);
        cb1.setGreen(0);
        cb1.setBlue(0);

        ChannelBinding cb2 = new ChannelBinding();
        cb2.setFamily(new Family(Family.VALUE_LINEAR));
        cb2.setCoefficient(CH2_COEFFICIENT);
        cb2.setActive(CH2_ACTIVE);
        cb2.setInputStart(CH2_WINDOW_START);
        cb2.setInputEnd(CH2_WINDOW_END);
        cb2.setRed(0);
        cb2.setGreen(255);
        cb2.setBlue(0);

        ChannelBinding cb3 = new ChannelBinding();
        cb3.setFamily(new Family(Family.VALUE_LINEAR));
        cb3.setCoefficient(CH3_COEFFICIENT);
        cb3.setActive(CH3_ACTIVE);
        cb3.setInputStart(CH3_WINDOW_START);
        cb3.setInputEnd(CH3_WINDOW_END);
        cb3.setRed(0);
        cb3.setGreen(0);
        cb3.setBlue(255);

        cbs = new ChannelBinding[] { cb1, cb2, cb3 };
        when(renderer.getChannelBindings()).thenReturn(cbs);
        when(renderer.getPixelsTypeLowerBound(0)).thenReturn(CH1_WINDOW_MIN);
        when(renderer.getPixelsTypeLowerBound(1)).thenReturn(CH2_WINDOW_MIN);
        when(renderer.getPixelsTypeLowerBound(2)).thenReturn(CH3_WINDOW_MIN);
        when(renderer.getPixelsTypeUpperBound(0)).thenReturn(CH1_WINDOW_MAX);
        when(renderer.getPixelsTypeUpperBound(1)).thenReturn(CH2_WINDOW_MAX);
        when(renderer.getPixelsTypeUpperBound(2)).thenReturn(CH3_WINDOW_MAX);

        CodomainChain cc = new CodomainChain(0, 1);
        when(renderer.getCodomainChain(0)).thenReturn(cc);
        when(renderer.getCodomainChain(1)).thenReturn(cc);
        when(renderer.getCodomainChain(2)).thenReturn(cc);

        rdef = new RenderingDef();
        rdef.setDefaultT(DEFAULT_T);
        rdef.setDefaultZ(DEFAULT_Z);
        RenderingModel model = new RenderingModel(RenderingModel.VALUE_RGB);
        rdef.setModel(model);

        WellSampleI ws = new WellSampleI(WELL_SAMPLE_ID, true);
        WellI well = new WellI(WELL_ID, true);
        ws.setWell(well);
        List<WellSample> wsList = new ArrayList<WellSample>();
        wsList.add(ws);
        when(image.copyWellSamples()).thenReturn(wsList);
        when(image.sizeOfWellSamples()).thenReturn(1);

        ObjectiveSettingsI os = new ObjectiveSettingsI();
        ObjectiveI obj = new ObjectiveI();
        obj.setNominalMagnification(rtypes.rdouble(NOMINAL_MAGNIFICATION));
        os.setObjective(obj);
        image.setObjectiveSettings(os);
    }

    @Test
    public void testImageDataStd() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        Assert.assertEquals(basicObj, imgData);
    }

    @Test
    public void testImageDataMultipleProjects() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        ProjectI project2 = new ProjectI();
        project2.setName(rtypes.rstring("proj2 name"));
        project2.setDescription(rtypes.rstring("proj2 desc"));
        ProjectDatasetLinkI projLink2 = new ProjectDatasetLinkI();
        projLink2.setParent(project2);
        image.linkedDatasetList().get(0).addProjectDatasetLink(projLink2);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject multProjCorrect = imgData.copy();
        multProjCorrect.getJsonObject("meta").put("projectName",
                "Multiple");
        multProjCorrect.getJsonObject("meta").remove("projectId");
        multProjCorrect.getJsonObject("meta").remove("projectDescription");
        Assert.assertEquals(basicObj, multProjCorrect);
    }

    @Test
    public void testImageDataMultipleDatasetsAnd() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        DatasetI ds2 = new DatasetI();
        ds2.setName(rtypes.rstring("ds2 name"));
        ds2.setDescription(rtypes.rstring("ds2 desc"));
        ProjectDatasetLinkI projLink2 = new ProjectDatasetLinkI();
        projLink2.setParent(image.linkedDatasetList().get(0)
                .linkedProjectList().get(0));
        ds2.addProjectDatasetLink(projLink2);
        DatasetImageLinkI dsLink2 = new DatasetImageLinkI();
        dsLink2.setParent(ds2);
        image.addDatasetImageLink(dsLink2);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject multDsCorrect = imgData.copy();
        multDsCorrect.getJsonObject("meta").put("datasetName", "Multiple");
        multDsCorrect.getJsonObject("meta").remove("datasetId");
        multDsCorrect.getJsonObject("meta").remove("datasetDescription");
        Assert.assertEquals(basicObj, multDsCorrect);
    }

    @Test
    public void testImageDataMultipleDatasetsAndProjects() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        ProjectI project2 = new ProjectI(123, true);
        project2.setName(rtypes.rstring("proj2 name"));
        project2.setDescription(rtypes.rstring("proj2 desc"));
        ProjectDatasetLinkI projLink2 = new ProjectDatasetLinkI(1234,
                true);
        projLink2.setParent(project2);

        DatasetI ds2 = new DatasetI(123, true);
        ds2.setName(rtypes.rstring("ds2 name"));
        ds2.setDescription(rtypes.rstring("ds2 desc"));
        projLink2.setParent(project2);
        ds2.addProjectDatasetLink(projLink2);
        DatasetImageLinkI dsLink2 = new DatasetImageLinkI();
        dsLink2.setParent(ds2);
        image.addDatasetImageLink(dsLink2);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject multDsProjCorrect = imgData.copy();
        multDsProjCorrect.getJsonObject("meta").put("datasetName",
                "Multiple");
        multDsProjCorrect.getJsonObject("meta").remove("datasetId");
        multDsProjCorrect.getJsonObject("meta")
                .remove("datasetDescription");

        multDsProjCorrect.getJsonObject("meta").put("projectName",
                "Multiple");
        multDsProjCorrect.getJsonObject("meta").remove("projectId");
        multDsProjCorrect.getJsonObject("meta")
                .remove("projectDescription");
        Assert.assertEquals(basicObj, multDsProjCorrect);
    }

    @Test
    public void testImageDataZoomLvl() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        List<List<Integer>> resLvlDescs = new ArrayList<List<Integer>>();
        resLvlDescs.add(Arrays.asList(512, 1024));
        resLvlDescs.add(Arrays.asList(128, 256));
        resLvlDescs.add(Arrays.asList(32, 64));
        when(renderer.getResolutionDescriptions()).thenReturn(resLvlDescs);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject zoomLvlsCorrect = imgData.copy();
        JsonObject zoomLvls = new JsonObject();
        zoomLvls.put("0", 1.0);
        zoomLvls.put("1", 0.25);
        zoomLvls.put("2", 0.0625);
        zoomLvlsCorrect.put("zoomLevelScaling", zoomLvls);
        Assert.assertEquals(basicObj, zoomLvlsCorrect);
    }

    @Test
    public void testImageDataPixelRange() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        when(pixelBuffer.getByteWidth()).thenReturn(2);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject pixRangeCorrect = imgData.copy();
        JsonArray pixRange = new JsonArray();
        pixRange.add(0);
        pixRange.add(65535); // 2^(8*2) - 1
        pixRangeCorrect.put("pixel_range", pixRange);
        Assert.assertEquals(basicObj, pixRangeCorrect);

        when(pixelBuffer.isSigned()).thenReturn(true);
        basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);

        pixRange = new JsonArray();
        pixRange.add(-32768);
        pixRange.add(32767);
        pixRangeCorrect.put("pixel_range", pixRange);
        Assert.assertEquals(basicObj, pixRangeCorrect);
    }

    @Test
    public void testImageDataSplitChannel() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        List<Channel> channels = pixels.copyChannels();
        channels.remove(2);
        pixels.clearChannels();
        pixels.addAllChannelSet(channels);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject splitChannelCorrect = imgData.copy();
        JsonObject g = new JsonObject();
        g.put("width", 1030);
        g.put("height", 1028);
        g.put("border", 2);
        g.put("gridx", 2);
        g.put("gridy", 1);
        JsonObject c = new JsonObject();
        c.put("width", 1030);
        c.put("height", 2054);
        c.put("border", 2);
        c.put("gridx", 2);
        c.put("gridy", 2);
        JsonObject splitChannel = new JsonObject();
        splitChannel.put("g", g);
        splitChannel.put("c", c);
        splitChannelCorrect.put("split_channel", splitChannel);
        splitChannelCorrect.getJsonArray("channels").remove(2);

        Assert.assertEquals(basicObj, splitChannelCorrect);
    }

    @Test
    public void testImageDataInverted() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        ReverseIntensityContext reverseIntensityCtx = new ReverseIntensityContext();
        List<CodomainMapContext> ctxList = new ArrayList<CodomainMapContext>();
        ctxList.add(reverseIntensityCtx);
        CodomainChain cc = mock(CodomainChain.class);
        when(cc.getContexts()).thenReturn(ctxList);
        when(renderer.getCodomainChain(0)).thenReturn(cc);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject invertedCorrect = imgData.copy();
        JsonObject channel = invertedCorrect.getJsonArray("channels")
                .getJsonObject(0);
        channel.put("inverted", true);
        channel.put("reverseIntensity", true);
        Assert.assertEquals(basicObj, invertedCorrect);
    }

    @Test
    public void testImageDataPixelSize() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        pixels.setPhysicalSizeX(new LengthI(3.0, UNITS.MILLIMETER));
        pixels.setPhysicalSizeY(new LengthI(4.0, UNITS.CENTIMETER));
        pixels.setPhysicalSizeZ(new LengthI(5.0, UNITS.NANOMETER));

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject pixelSizeCorrect = imgData.copy();
        JsonObject pixSize = pixelSizeCorrect.getJsonObject("pixel_size");
        pixSize.put("x", 3000.0);
        pixSize.put("y", 40000.0);
        pixSize.put("z", 0.005);
        Assert.assertEquals(basicObj, pixelSizeCorrect);
    }

    @Test
    public void testImageDataTimestampOnImage() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);
        image.setAcquisitionDate(rtypes.rtime(22222222));

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject timestampCorrect = imgData.copy();
        timestampCorrect.getJsonObject("meta").put("imageTimestamp",
                22222);
        Assert.assertEquals(basicObj, timestampCorrect);
    }

    @Test
    public void testImageDataChannels() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ChannelBinding cb = cbs[0];
        cb.setRed(0);
        cb.setGreen(17);
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject channelsCorrect = imgData.copy();
        channelsCorrect.getJsonArray("channels").getJsonObject(0).put("color", "001100");
        Assert.assertEquals(basicObj, channelsCorrect);
    }

    @Test
    public void testImageData2Channels() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        pixels.removeChannel(pixels.getChannel(0));

        ChannelBinding cb2 = new ChannelBinding();
        cb2.setFamily(new Family(Family.VALUE_LINEAR));
        cb2.setCoefficient(CH2_COEFFICIENT);
        cb2.setActive(CH2_ACTIVE);
        cb2.setInputStart(CH2_WINDOW_START);
        cb2.setInputEnd(CH2_WINDOW_END);
        cb2.setRed(0);
        cb2.setGreen(255);
        cb2.setBlue(0);

        ChannelBinding cb3 = new ChannelBinding();
        cb3.setFamily(new Family(Family.VALUE_LINEAR));
        cb3.setCoefficient(CH3_COEFFICIENT);
        cb3.setActive(CH3_ACTIVE);
        cb3.setInputStart(CH3_WINDOW_START);
        cb3.setInputEnd(CH3_WINDOW_END);
        cb3.setRed(0);
        cb3.setGreen(0);
        cb3.setBlue(255);

        ChannelBinding[] cbs = new ChannelBinding[] { cb2, cb3 };
        when(renderer.getChannelBindings()).thenReturn(cbs);
        when(renderer.getPixelsTypeLowerBound(1)).thenReturn(CH2_WINDOW_MIN);
        when(renderer.getPixelsTypeLowerBound(2)).thenReturn(CH3_WINDOW_MIN);
        when(renderer.getPixelsTypeUpperBound(1)).thenReturn(CH2_WINDOW_MAX);
        when(renderer.getPixelsTypeUpperBound(2)).thenReturn(CH3_WINDOW_MAX);

        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);
        JsonObject channelsCorrect = imgData.copy();
        channelsCorrect.getJsonArray("channels").remove(0);
        channelsCorrect.getJsonObject("split_channel").getJsonObject("g").put("gridy", 1);
        channelsCorrect.getJsonObject("split_channel").getJsonObject("g").put("height", 1028);
        Assert.assertEquals(basicObj, channelsCorrect);
    }

    @Test
    public void testImageDataNullPixelSizes() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        pixels.setPhysicalSizeX(null);
        pixels.setPhysicalSizeY(null);

        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null, null, null, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(image, pixels,
                creationEvent, owner, permissions, pixelBuffer,
                renderer, rdef);

        JsonObject nullPhysSizeCorrect = imgData.copy();
        JsonObject pixelSize = nullPhysSizeCorrect.getJsonObject("pixel_size");
        pixelSize.putNull("x");
        pixelSize.putNull("y");
        Assert.assertEquals(basicObj, nullPhysSizeCorrect);
    }
}
