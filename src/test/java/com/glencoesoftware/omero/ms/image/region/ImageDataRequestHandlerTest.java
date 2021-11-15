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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import omero.model.PixelsTypeI;
import ome.units.UNITS;
import omero.ApiUsageException;
import omero.model.ChannelBinding;
import omero.model.ChannelBindingI;
import omero.model.ChannelI;
import omero.model.DatasetI;
import omero.model.Event;
import omero.model.EventI;
import omero.model.Experimenter;
import omero.model.ExperimenterI;
import omero.model.Family;
import omero.model.FamilyI;
import omero.model.ImageI;
import omero.model.LengthI;
import omero.model.LogicalChannelI;
import omero.model.ObjectiveI;
import omero.model.ObjectiveSettingsI;
import omero.model.Permissions;
import omero.model.PermissionsI;
import omero.model.Pixels;
import omero.model.PixelsI;
import omero.model.ProjectI;
import omero.model.RenderingDef;
import omero.model.RenderingDefI;
import omero.model.RenderingModel;
import omero.model.RenderingModelI;
import omero.model.ReverseIntensityContext;
import omero.model.ReverseIntensityContextI;
import omero.model.StatsInfoI;
import omero.model.WellI;
import omero.model.WellSampleI;
import omero.model.enums.UnitsLength;
import omero.util.IceMapper;

import org.junit.Assert;
import org.junit.Before;

import static omero.rtypes.rbool;
import static omero.rtypes.rdouble;
import static omero.rtypes.rint;
import static omero.rtypes.rlong;
import static omero.rtypes.rstring;
import static omero.rtypes.rtime;

public class ImageDataRequestHandlerTest extends AbstractZarrPixelBufferTest {

    ImageI image;
    PixelBuffer pixelBuffer;
    RenderingDef rdef;

    public static String OWNER_FIRST_NAME = "firstname";
    public static String OWNER_LAST_NAME = "lastname";
    public static Long OWNER_ID = 123l;

    public static long IMAGE_ID = 1l;
    public static String IMAGE_NAME = "test name";
    public static String IMAGE_DESC = "test image description";
    public static long IMAGE_TIMESTAMP = 12345l;

    public static long DATASET_ID_1 = 100l;
    public static String DATASET_NAME_1 = "ds name 1";
    public static String DATASET_DESC_1 = "ds description 1";

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

    public static Integer TILE_WIDTH = 1024;
    public static Integer TILE_HEIGHT = 1024;
    public static int PIXELS_SIZE_X = 2048;
    public static int PIXELS_SIZE_Y = 4096;
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

    public static int SPLIT_CH_C_BORDER = 2;
    public static int SPLIT_CH_C_GRIDX = 2;
    public static int SPLIT_CH_C_GRIDY = 2;
    public static int SPLIT_CH_C_HEIGHT = 8198;
    public static int SPLIT_CH_C_WIDTH = 4102;

    public static int SPLIT_CH_G_BORDER = 2;
    public static int SPLIT_CH_G_GRIDX = 2;
    public static int SPLIT_CH_G_GRIDY = 2;
    public static int SPLIT_CH_G_HEIGHT = 8198;
    public static int SPLIT_CH_G_WIDTH = 4102;

    public static int DEFAULT_T = 0;
    public static int DEFAULT_Z = 1;
    public static boolean INVERT_AXIS = false;
    public static String MODEL = "color";
    public static String PROJECTION = "normal";

    JsonObject imgData;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects and
     * <code>ome.model</code> server side Hibernate backed objects.
     */
    protected final IceMapper mapper = new IceMapper();

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
        ch3Window.put("min", 0);
        ch3Window.put("max", 255);
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

    private void createPixelBuffer() throws IOException, ApiUsageException {
        Path output = writeTestZarr(
                PIXELS_SIZE_T, PIXELS_SIZE_C, PIXELS_SIZE_Z,
                PIXELS_SIZE_Y, PIXELS_SIZE_X, PIX_TYPE_STR, RES_LVL_COUNT);
        pixelBuffer = new ZarrPixelBuffer(
            (ome.model.core.Pixels) mapper.reverse(image.getPrimaryPixels()),
            output.resolve("0"), 1024);
    }

    @Before
    public void setup() throws IOException, ApiUsageException {
        setupStdJson();

        Event creationEvent = new EventI();
        creationEvent.setTime(rtime(IMAGE_TIMESTAMP*1000));
        Experimenter owner = new ExperimenterI();
        owner.setFirstName(rstring(OWNER_FIRST_NAME));
        owner.setLastName(rstring(OWNER_LAST_NAME));
        owner.setId(rlong(OWNER_ID));

        image = new ImageI();
        image.setId(rlong(IMAGE_ID));
        image.setName(rstring(IMAGE_NAME));
        image.setDescription(rstring(IMAGE_DESC));
        image.getDetails().setOwner(owner);
        image.getDetails().setCreationEvent(creationEvent);
        DatasetI ds1 = new DatasetI(DATASET_ID_1, true);
        ds1.setName(rstring(DATASET_NAME_1));
        ds1.setDescription(rstring(DATASET_DESC_1));
        ProjectI proj_1 = new ProjectI(PROJECT_ID_1, true);
        proj_1.setName(rstring(PROJECT_NAME_1));
        proj_1.setDescription(rstring(PROJECT_DESC_1));
        image.linkDataset(ds1);
        ds1.linkProject(proj_1);

        Pixels pixels = new PixelsI(PIXELS_ID, true);
        PixelsTypeI pixType = new PixelsTypeI();
        pixType.setValue(rstring(PIX_TYPE_STR));
        pixels.setPixelsType(pixType);
        pixType.setBitSize(rint(8));
        pixels.setPhysicalSizeX(new LengthI(PHYSICAL_SIZE_X, UnitsLength.MICROMETER));
        pixels.setPhysicalSizeY(new LengthI(PHYSICAL_SIZE_Y, UnitsLength.MICROMETER));
        pixels.setSizeX(rint(PIXELS_SIZE_X));
        pixels.setSizeY(rint(PIXELS_SIZE_Y));
        pixels.setSizeZ(rint(PIXELS_SIZE_Z));
        pixels.setSizeC(rint(PIXELS_SIZE_C));
        pixels.setSizeT(rint(PIXELS_SIZE_T));
        image.addPixels(pixels);

        ChannelI channel1 = new ChannelI();
        channel1.setRed(rint(255));
        channel1.setGreen(rint(0));
        channel1.setBlue(rint(0));
        LogicalChannelI logCh1 = new LogicalChannelI();
        logCh1.setName(rstring(CH1_LABEL));
        channel1.setLogicalChannel(logCh1);
        StatsInfoI statsInfo1 = new StatsInfoI();
        statsInfo1.setGlobalMin(rdouble(CH1_WINDOW_MIN));
        statsInfo1.setGlobalMax(rdouble(CH1_WINDOW_MAX));
        channel1.setStatsInfo(statsInfo1);

        ChannelI channel2 = new ChannelI();
        channel2.setRed(rint(0));
        channel2.setGreen(rint(255));
        channel2.setBlue(rint(0));
        LogicalChannelI logCh2 = new LogicalChannelI();
        logCh2.setName(rstring(CH2_LABEL));
        channel2.setLogicalChannel(logCh2);
        StatsInfoI statsInfo2 = new StatsInfoI();
        statsInfo2.setGlobalMin(rdouble(CH2_WINDOW_MIN));
        statsInfo2.setGlobalMax(rdouble(CH2_WINDOW_MAX));
        channel2.setStatsInfo(statsInfo2);

        ChannelI channel3 = new ChannelI();
        channel3.setRed(rint(0));
        channel3.setGreen(rint(0));
        channel3.setBlue(rint(255));
        LogicalChannelI logCh3 = new LogicalChannelI();
        logCh3.setName(rstring(CH3_LABEL));
        channel3.setLogicalChannel(logCh3);

        pixels.addChannel(channel1);
        pixels.addChannel(channel2);
        pixels.addChannel(channel3);

        Permissions permissions = new PermissionsI("rwrwrw");
        image.getDetails().setPermissions(permissions);

        createPixelBuffer();

        ChannelBinding cb1 = new ChannelBindingI();
        Family family = new FamilyI();
        family.setValue(rstring("linear"));
        cb1.setFamily(family);
        cb1.setCoefficient(rdouble(CH1_COEFFICIENT));
        cb1.setActive(rbool(CH1_ACTIVE));
        cb1.setInputStart(rdouble(CH1_WINDOW_START));
        cb1.setInputEnd(rdouble(CH1_WINDOW_END));
        cb1.setRed(rint(255));
        cb1.setGreen(rint(0));
        cb1.setBlue(rint(0));

        ChannelBinding cb2 = new ChannelBindingI();
        cb2.setFamily(family);
        cb2.setCoefficient(rdouble(CH2_COEFFICIENT));
        cb2.setActive(rbool(CH2_ACTIVE));
        cb2.setInputStart(rdouble(CH2_WINDOW_START));
        cb2.setInputEnd(rdouble(CH2_WINDOW_END));
        cb2.setRed(rint(0));
        cb2.setGreen(rint(255));
        cb2.setBlue(rint(0));

        ChannelBinding cb3 = new ChannelBindingI();
        cb3.setFamily(family);
        cb3.setCoefficient(rdouble(CH3_COEFFICIENT));
        cb3.setActive(rbool(CH3_ACTIVE));
        cb3.setInputStart(rdouble(CH3_WINDOW_START));
        cb3.setInputEnd(rdouble(CH3_WINDOW_END));
        cb3.setRed(rint(0));
        cb3.setGreen(rint(0));
        cb3.setBlue(rint(255));

        rdef = new RenderingDefI();

        rdef.addChannelBinding(cb1);
        rdef.addChannelBinding(cb2);
        rdef.addChannelBinding(cb3);

        rdef.setDefaultT(rint(DEFAULT_T));
        rdef.setDefaultZ(rint(DEFAULT_Z));
        RenderingModel model = new RenderingModelI();
        model.setValue(rstring("rgb"));
        rdef.setModel(model);

        WellSampleI ws = new WellSampleI(WELL_SAMPLE_ID, true);
        WellI well = new WellI(WELL_ID, true);
        ws.setWell(well);
        image.addWellSample(ws);

        ObjectiveSettingsI os = new ObjectiveSettingsI();
        ObjectiveI obj = new ObjectiveI();
        obj.setNominalMagnification(rdouble(NOMINAL_MAGNIFICATION));
        os.setObjective(obj);
        image.setObjectiveSettings(os);
    }

    @Test
    public void testImageDataStd() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        Assert.assertEquals(basicObj, imgData);
    }

    @Test
    public void testImageDataMultipleProjects() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        ProjectI project2 = new ProjectI();
        project2.setName(rstring("proj2 name"));
        project2.setDescription(rstring("proj2 desc"));
        image.linkedDatasetList().get(0).linkProject(project2);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject multProjCorrect = imgData.copy();
        multProjCorrect.getJsonObject("meta").put("projectName",
                "Multiple");
        multProjCorrect.getJsonObject("meta").remove("projectId");
        multProjCorrect.getJsonObject("meta").remove("projectDescription");
        Assert.assertEquals(basicObj, multProjCorrect);
    }

    @Test
    public void testImageDataMultipleDatasets()
            throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        DatasetI ds2 = new DatasetI();
        ds2.setName(rstring("ds2 name"));
        ds2.setDescription(rstring("ds2 desc"));
        image.linkDataset(ds2);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject multDsCorrect = imgData.copy();
        multDsCorrect.getJsonObject("meta").put("datasetName", "Multiple");
        multDsCorrect.getJsonObject("meta").remove("datasetId");
        multDsCorrect.getJsonObject("meta").remove("datasetDescription");
        Assert.assertEquals(basicObj, multDsCorrect);
    }

    @Test
    public void testImageDataMultipleDatasetsAndProjects()
            throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        ProjectI project2 = new ProjectI(123, true);
        project2.setName(rstring("proj2 name"));
        project2.setDescription(rstring("proj2 desc"));

        DatasetI ds2 = new DatasetI(123, true);
        ds2.setName(rstring("ds2 name"));
        ds2.setDescription(rstring("ds2 desc"));
        ds2.linkProject(project2);
        image.linkDataset(ds2);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
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
    public void testImageDataPixelRange()
            throws ApiUsageException, IOException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject pixRangeCorrect = imgData.copy();
        JsonArray pixRange = new JsonArray();
        pixRange.add(0);
        pixRange.add(255);
        pixRangeCorrect.put("pixel_range", pixRange);
        Assert.assertEquals(basicObj, pixRangeCorrect);

        // Need new request handler because the Ice object mapper caches
        // object instances and we're only changing a single value
        reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        image.getPrimaryPixels().getPixelsType().setValue(rstring("int8"));
        createPixelBuffer();  // Resets pixel buffer with int8 pixels type
        basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);

        pixRange = pixRangeCorrect.getJsonArray("pixel_range");
        pixRange.set(0, -128);
        pixRange.set(1, 127);
        JsonObject window = pixRangeCorrect
                .getJsonArray("channels")
                .getJsonObject(2)
                .getJsonObject("window");
        window.put("min", -128);
        window.put("max", 127);
        pixRangeCorrect.getJsonObject("meta").put("pixelsType", "int8");
        Assert.assertEquals(basicObj, pixRangeCorrect);
    }

    @Test
    public void testImageDataInverted() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        ReverseIntensityContext reverseIntensityCtx =
                new ReverseIntensityContextI();
        rdef.getChannelBinding(0).addCodomainMapContext(reverseIntensityCtx);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject invertedCorrect = imgData.copy();
        JsonObject channel = invertedCorrect.getJsonArray("channels")
                .getJsonObject(0);
        channel.put("inverted", true);
        channel.put("reverseIntensity", true);
        Assert.assertEquals(basicObj, invertedCorrect);
    }

    @Test
    public void testImageDataPixelSize() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        Pixels pixels = image.getPrimaryPixels();
        pixels.setPhysicalSizeX(new LengthI(3.0, UNITS.MILLIMETER));
        pixels.setPhysicalSizeY(new LengthI(4.0, UNITS.CENTIMETER));
        pixels.setPhysicalSizeZ(new LengthI(5.0, UNITS.NANOMETER));

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject pixelSizeCorrect = imgData.copy();
        JsonObject pixSize = pixelSizeCorrect.getJsonObject("pixel_size");
        pixSize.put("x", 3000.0);
        pixSize.put("y", 40000.0);
        pixSize.put("z", 0.005);
        Assert.assertEquals(basicObj, pixelSizeCorrect);
    }

    @Test
    public void testImageDataTimestampOnImage() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);
        image.setAcquisitionDate(rtime(22222222));

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject timestampCorrect = imgData.copy();
        timestampCorrect.getJsonObject("meta").put("imageTimestamp",
                22222);
        Assert.assertEquals(basicObj, timestampCorrect);
    }

    @Test
    public void testImageDataChannels() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ChannelBinding cb = rdef.getChannelBinding(0);
        cb.setRed(rint(0));
        cb.setGreen(rint(17));
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject channelsCorrect = imgData.copy();
        channelsCorrect.getJsonArray("channels").getJsonObject(0).put("color", "001100");
        Assert.assertEquals(basicObj, channelsCorrect);
    }

    @Test
    public void testImageData2Channels() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        Pixels pixels = image.getPrimaryPixels();
        pixels.removeChannel(pixels.getChannel(0));

        ChannelBinding cb2 = new ChannelBindingI();
        Family family = new FamilyI();
        family.setValue(rstring("linear"));
        cb2.setFamily(family);
        cb2.setCoefficient(rdouble(CH2_COEFFICIENT));
        cb2.setActive(rbool(CH2_ACTIVE));
        cb2.setInputStart(rdouble(CH2_WINDOW_START));
        cb2.setInputEnd(rdouble(CH2_WINDOW_END));
        cb2.setRed(rint(0));
        cb2.setGreen(rint(255));
        cb2.setBlue(rint(0));

        ChannelBinding cb3 = new ChannelBindingI();
        cb3.setFamily(family);
        cb3.setCoefficient(rdouble(CH3_COEFFICIENT));
        cb3.setActive(rbool(CH3_ACTIVE));
        cb3.setInputStart(rdouble(CH3_WINDOW_START));
        cb3.setInputEnd(rdouble(CH3_WINDOW_END));
        cb3.setRed(rint(0));
        cb3.setGreen(rint(0));
        cb3.setBlue(rint(255));

        rdef.removeChannelBinding(rdef.getChannelBinding(0));

        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);
        JsonObject channelsCorrect = imgData.copy();
        channelsCorrect.getJsonArray("channels").remove(0);
        channelsCorrect.getJsonObject("split_channel").getJsonObject("g").put("gridy", 1);
        channelsCorrect.getJsonObject("split_channel").getJsonObject("g").put("height", 4100);
        Assert.assertEquals(basicObj, channelsCorrect);
    }

    @Test
    public void testImageDataNullPixelSizes() throws ApiUsageException {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        Pixels pixels = image.getPrimaryPixels();
        pixels.setPhysicalSizeX(null);
        pixels.setPhysicalSizeY(null);

        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(
                ctx, null, 0, true);

        JsonObject basicObj = reqHandler.populateImageData(
                image, pixelBuffer, rdef);

        JsonObject nullPhysSizeCorrect = imgData.copy();
        JsonObject pixelSize = nullPhysSizeCorrect.getJsonObject("pixel_size");
        pixelSize.putNull("x");
        pixelSize.putNull("y");
        Assert.assertEquals(basicObj, nullPhysSizeCorrect);
    }
}
