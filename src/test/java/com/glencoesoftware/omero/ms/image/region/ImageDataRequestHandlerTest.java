package com.glencoesoftware.omero.ms.image.region;

import static org.mockito.Mockito.*;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import ome.model.display.ChannelBinding;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import omero.model.PixelsTypeI;
import ome.model.enums.RenderingModel;
import omeis.providers.re.Renderer;
import omeis.providers.re.codomain.CodomainChain;
import omero.ServerError;
import omero.rtypes;
import omero.api.RawPixelsStorePrx;
import omero.model.ChannelI;
import omero.model.DatasetI;
import omero.model.DatasetImageLinkI;
import omero.model.Experimenter;
import omero.model.ExperimenterI;
import omero.model.ImageI;
import omero.model.LengthI;
import omero.model.LogicalChannelI;
import omero.model.Permissions;
import omero.model.PermissionsI;
import omero.model.PixelsI;
import omero.model.ProjectDatasetLinkI;
import omero.model.ProjectI;
import omero.model.StatsInfoI;
import omero.model.WellSampleI;
import omero.model.enums.UnitsLength;

import org.junit.Assert;
import org.junit.Before;

public class ImageDataRequestHandlerTest {

    ImageI image;
    PixelsI pixels;
    Experimenter owner;
    Optional<WellSampleI> wellSample;
    Permissions permissions;
    PixelBuffer pixelBuffer;
    RawPixelsStorePrx rp;
    Renderer renderer;
    RenderingDef rdef;

    public static String OWNER_FIRST_NAME = "firstname";
    public static String OWNER_LAST_NAME = "lastname";

    public static long IMAGE_ID = 1l;
    public static String IMAGE_NAME = "test name";
    public static String IMAGE_DESC = "test image description";

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

    public static String CHANNEL_NAME_1 = "channel name 1";
    public static String CHANNEL_NAME_2 = "channel name 2";
    public static String CHANNEL_NAME_3 = "channel name 3";

    public static Integer RES_LVL_COUNT = 5;
    public static Integer TILE_WIDTH = 256;
    public static Integer TILE_HEIGHT = 512;
    public static int PIXELS_SIZE_X = 512;
    public static int PIXELS_SIZE_Y = 1024;
    public static int PIXELS_SIZE_Z = 1;
    public static int PIXELS_SIZE_C = 3;
    public static int PIXELS_SIZE_T = 1;

    public static int BYTE_WIDTH = 1;


    JsonObject stdCorrect = new JsonObject("{" +
            "    \"id\": " + Long.toString(IMAGE_ID) + "," +
            "    \"meta\": {\n" +
            "        \"imageName\": \"" + IMAGE_NAME + "\"," +
            "        \"imageDescription\": \"" + IMAGE_DESC + "\"," +
            "        \"imageAuthor\": \"" + OWNER_FIRST_NAME + " " + OWNER_LAST_NAME + "\"," +
            "        \"projectName\": \"" + PROJECT_NAME_1 + "\"," +
            "        \"projectId\": " + Long.toString(PROJECT_ID_1) + "," +
            "        \"projectDescription\": \"" + PROJECT_DESC_1 + "\"," +
            "        \"datasetName\": \"" + DATASET_NAME_1 + "\"," +
            "        \"datasetId\": " + Long.toString(DATASET_ID_1) +  "," +
            "        \"datasetDescription\": \"" + DATASET_DESC_1 + "\"," +
            //"        \"wellSampleId\": \"\",\n" +
            //"        \"wellId\": \"\",\n" +
            //"        \"imageTimestamp\": 1614187774.0,\n" +
            "        \"imageId\": " + Long.toString(IMAGE_ID) + ",\n" +
            "        \"pixelsType\": \"" + PIX_TYPE_STR + "\"" +
            "    },\n" +
            "    \"perms\": {\n" +
            "        \"canAnnotate\": true,\n" +
            "        \"canEdit\": true,\n" +
            "        \"canDelete\": true,\n" +
            "        \"canLink\": true\n" +
            "    },\n" +
            "    \"tiles\": true,\n" +
            "    \"tile_size\": {\n" +
            "        \"width\": " + TILE_WIDTH + ",\n" +
            "        \"height\": " + TILE_HEIGHT + "\n" +
            "    },\n" +
            "    \"levels\": 5,\n" +
            "    \"zoomLevelScaling\": {\n" +
            "        \"0\": 1.0,\n" +
            "        \"1\": 0.5,\n" +
            "        \"2\": 0.25,\n" +
            "        \"3\": 0.125,\n" +
            "        \"4\": 0.0625\n" +
            "    },\n" +
            "    \"interpolate\": true,\n" +
            "    \"size\": {\n" +
            "        \"width\": 512,\n" +
            "        \"height\": 1024,\n" +
            "        \"z\": 1,\n" +
            "        \"t\": 1,\n" +
            "        \"c\": 3\n" +
            "    },\n" +
            "    \"pixel_size\": {\n" +
            "        \"x\": 1.0,\n" +
            "        \"y\": 2.0,\n" +
            "        \"z\": null\n" +
            "    },\n" +
            "    \"init_zoom\": 0,\n" +
            "    \"pixel_range\": [\n" +
            "        0,\n" +
            "        255\n" +
            "    ],\n" +
            "    \"channels\": [\n" +
            "        {\n" +
            "            \"emissionWave\": null,\n" +
            "            \"label\": \"" + CHANNEL_NAME_1 + "\",\n" +
            "            \"color\": \"FF0000\",\n" +
            "            \"inverted\": false,\n" +
            "            \"reverseIntensity\": false,\n" +
            "            \"family\": \"linear\",\n" +
            "            \"coefficient\": 1.1,\n" +
            "            \"window\": {\n" +
            "                \"min\": -10.0,\n" +
            "                \"max\": 10.0,\n" +
            "                \"start\": 0.0,\n" +
            "                \"end\": 30.0\n" +
            "            },\n" +
            "            \"active\": true\n" +
            "        },\n" +
            "        {\n" +
            "            \"emissionWave\": null,\n" +
            "            \"label\": \"" + CHANNEL_NAME_2 + "\",\n" +
            "            \"color\": \"00FF00\",\n" +
            "            \"inverted\": false,\n" +
            "            \"reverseIntensity\": false,\n" +
            "            \"family\": \"linear\",\n" +
            "            \"coefficient\": 1.2,\n" +
            "            \"window\": {\n" +
            "                \"min\": -20.0,\n" +
            "                \"max\": 20.0,\n" +
            "                \"start\": 0.0,\n" +
            "                \"end\": 31.0\n" +
            "            },\n" +
            "            \"active\": true\n" +
            "        },\n" +
            "        {\n" +
            "            \"emissionWave\": null,\n" +
            "            \"label\": \"" + CHANNEL_NAME_3 + "\",\n" +
            "            \"color\": \"0000FF\",\n" +
            "            \"inverted\": false,\n" +
            "            \"reverseIntensity\": false,\n" +
            "            \"family\": \"linear\",\n" +
            "            \"coefficient\": 1.3,\n" +
            "            \"window\": {\n" +
            "                \"min\": -30.0,\n" +
            "                \"max\": 30.0,\n" +
            "                \"start\": 0.0,\n" +
            "                \"end\": 32.0\n" +
            "            },\n" +
            "            \"active\": true\n" +
            "        }\n" +
            "    ],\n" +
            "    \"split_channel\": {\n" +
            "        \"g\": {\n" +
            "            \"width\": 1030,\n" +
            "            \"height\": 2054,\n" +
            "            \"border\": 2,\n" +
            "            \"gridx\": 2,\n" +
            "            \"gridy\": 2\n" +
            "        },\n" +
            "        \"c\": {\n" +
            "            \"width\": 1030,\n" +
            "            \"height\": 2054,\n" +
            "            \"border\": 2,\n" +
            "            \"gridx\": 2,\n" +
            "            \"gridy\": 2\n" +
            "        }\n" +
            "    },\n" +
            "    \"rdefs\": {\n" +
            "        \"model\": \"color\",\n" +
            "        \"projection\": \"normal\",\n" +
            "        \"defaultZ\": 1,\n" +
            "        \"defaultT\": 0,\n" +
            "        \"invertAxis\": false\n" +
            "    }\n" +
            "}");


    @Before
    public void setup() {

        owner = new ExperimenterI();
        owner.setFirstName(rtypes.rstring(OWNER_FIRST_NAME));
        owner.setLastName(rtypes.rstring(OWNER_LAST_NAME));

        image = new ImageI(IMAGE_ID, true);
        image.setName(rtypes.rstring(IMAGE_NAME));
        image.setDescription(rtypes.rstring(IMAGE_DESC));
        DatasetImageLinkI dslink1 = new DatasetImageLinkI(DATASET_LINK_ID_1, true);
        DatasetI ds1 = new DatasetI(DATASET_ID_1, true);
        ds1.setName(rtypes.rstring(DATASET_NAME_1));
        ds1.setDescription(rtypes.rstring(DATASET_DESC_1));
        dslink1.setParent(ds1);
        ProjectDatasetLinkI projLink1 = new ProjectDatasetLinkI(PROJECT_DS_LINK_ID_1, true);
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
        pixels.setPhysicalSizeX(new LengthI(1.0, UnitsLength.MICROMETER));
        pixels.setPhysicalSizeY(new LengthI(2.0, UnitsLength.MICROMETER));
        pixels.setSizeX(rtypes.rint(PIXELS_SIZE_X));
        pixels.setSizeY(rtypes.rint(PIXELS_SIZE_Y));

        ChannelI channel1 = new ChannelI();
        channel1.setRed(rtypes.rint(255));
        channel1.setGreen(rtypes.rint(0));
        channel1.setBlue(rtypes.rint(0));
        LogicalChannelI logCh1 = new LogicalChannelI();
        logCh1.setName(rtypes.rstring(CHANNEL_NAME_1));
        channel1.setLogicalChannel(logCh1);
        StatsInfoI statsInfo1 = new StatsInfoI();
        statsInfo1.setGlobalMin(rtypes.rdouble(-10.0));
        statsInfo1.setGlobalMax(rtypes.rdouble(10.0));
        channel1.setStatsInfo(statsInfo1);

        ChannelI channel2 = new ChannelI();
        channel2.setRed(rtypes.rint(0));
        channel2.setGreen(rtypes.rint(255));
        channel2.setBlue(rtypes.rint(0));
        LogicalChannelI logCh2 = new LogicalChannelI();
        logCh2.setName(rtypes.rstring(CHANNEL_NAME_2));
        channel2.setLogicalChannel(logCh2);
        StatsInfoI statsInfo2 = new StatsInfoI();
        statsInfo2.setGlobalMin(rtypes.rdouble(-20.0));
        statsInfo2.setGlobalMax(rtypes.rdouble(20.0));
        channel2.setStatsInfo(statsInfo2);

        ChannelI channel3 = new ChannelI();
        channel3.setRed(rtypes.rint(0));
        channel3.setGreen(rtypes.rint(0));
        channel3.setBlue(rtypes.rint(255));
        LogicalChannelI logCh3 = new LogicalChannelI();
        logCh3.setName(rtypes.rstring(CHANNEL_NAME_3));
        channel3.setLogicalChannel(logCh3);
        StatsInfoI statsInfo3 = new StatsInfoI();
        statsInfo3.setGlobalMin(rtypes.rdouble(-30.0));
        statsInfo3.setGlobalMax(rtypes.rdouble(30.0));
        channel3.setStatsInfo(statsInfo3);

        pixels.addChannel(channel1);
        pixels.addChannel(channel2);
        pixels.addChannel(channel3);

        permissions = new PermissionsI("rwrwrw");

        wellSample = Optional.empty();

        pixelBuffer = mock(PixelBuffer.class);
        when(pixelBuffer.getResolutionLevels()).thenReturn(RES_LVL_COUNT);
        when(pixelBuffer.getTileSize()).thenReturn(new Dimension(TILE_WIDTH, TILE_HEIGHT));
        when(pixelBuffer.getSizeX()).thenReturn(PIXELS_SIZE_X);
        when(pixelBuffer.getSizeY()).thenReturn(PIXELS_SIZE_Y);
        when(pixelBuffer.getSizeZ()).thenReturn(PIXELS_SIZE_Z);
        when(pixelBuffer.getSizeC()).thenReturn(PIXELS_SIZE_C);
        when(pixelBuffer.getSizeT()).thenReturn(PIXELS_SIZE_T);

        rp = mock(RawPixelsStorePrx.class);
        try {
            when(rp.getByteWidth()).thenReturn(BYTE_WIDTH);
            when(rp.isSigned()).thenReturn(false);
        } catch (ServerError e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        renderer = mock(Renderer.class);
        List<List<Integer>> resLvlDescs = new ArrayList<List<Integer>>();
        resLvlDescs.add(Arrays.asList(512, 1024));
        resLvlDescs.add(Arrays.asList(256, 512));
        resLvlDescs.add(Arrays.asList(128, 256));
        resLvlDescs.add(Arrays.asList(64, 128));
        resLvlDescs.add(Arrays.asList(32, 64));
        when(renderer.getResolutionDescriptions()).thenReturn(resLvlDescs);

        ChannelBinding cb1 = new ChannelBinding();
        cb1.setFamily(new Family(Family.VALUE_LINEAR));
        cb1.setCoefficient(1.1);
        cb1.setActive(true);
        cb1.setInputStart(0.0);
        cb1.setInputEnd(30.0);

        ChannelBinding cb2 = new ChannelBinding();
        cb2.setFamily(new Family(Family.VALUE_LINEAR));
        cb2.setCoefficient(1.2);
        cb2.setActive(true);
        cb2.setInputStart(0.0);
        cb2.setInputEnd(31.0);

        ChannelBinding cb3 = new ChannelBinding();
        cb3.setFamily(new Family(Family.VALUE_LINEAR));
        cb3.setCoefficient(1.3);
        cb3.setActive(true);
        cb3.setInputStart(0.0);
        cb3.setInputEnd(32.0);

        ChannelBinding[] cbs = new ChannelBinding[] {cb1, cb2, cb3};
        when(renderer.getChannelBindings()).thenReturn(cbs);
        when(renderer.getPixelsTypeLowerBound(0)).thenReturn(0.0);
        when(renderer.getPixelsTypeLowerBound(1)).thenReturn(1.0);
        when(renderer.getPixelsTypeLowerBound(2)).thenReturn(2.0);
        when(renderer.getPixelsTypeUpperBound(0)).thenReturn(0.0);
        when(renderer.getPixelsTypeUpperBound(1)).thenReturn(1.0);
        when(renderer.getPixelsTypeUpperBound(2)).thenReturn(2.0);

        CodomainChain cc = new CodomainChain(0, 1);
        when(renderer.getCodomainChain(0)).thenReturn(cc);
        when(renderer.getCodomainChain(0)).thenReturn(cc);
        when(renderer.getCodomainChain(0)).thenReturn(cc);

        rdef = new RenderingDef();
        rdef.setDefaultT(0);
        rdef.setDefaultZ(1);
        RenderingModel model = new RenderingModel(RenderingModel.VALUE_RGB);
        rdef.setModel(model);

    }

    @Test
    public void testImageDataStd() {
        ImageDataCtx ctx = new ImageDataCtx();
        ctx.imageId = IMAGE_ID;
        ImageDataRequestHandler reqHandler = new ImageDataRequestHandler(ctx,
                null,
                null,
                null,
                null,
                0,
                true);
        try {
            JsonObject basicObj = reqHandler.populateImageData(image,
                    pixels,
                    owner,
                    wellSample,
                    permissions,
                    pixelBuffer,
                    rp,
                    renderer,
                    rdef);
            System.out.println(basicObj.toString());
            System.out.println(stdCorrect.toString());
            Assert.assertEquals(basicObj, stdCorrect);
        } catch (ServerError e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail();
        }
    }

}
