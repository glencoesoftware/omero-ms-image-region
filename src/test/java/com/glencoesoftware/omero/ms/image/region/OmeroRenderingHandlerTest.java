package com.glencoesoftware.omero.ms.image.region;


import org.testng.annotations.Test;
import org.testng.Assert;

import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;

public class OmeroRenderingHandlerTest {

    @Test
    public void testCheckPlaneDef() {
        int x = 0;
        int y = 0;
        int w = 256;
        int h = 512;
        int t = 0;
        int z = 0;
        int sizeX = 1024;
        int sizeY = 1024;
        PlaneDef planeDef = new PlaneDef(PlaneDef.XY, t);
        planeDef.setZ(z);
        RegionDef region = new RegionDef(x, y, w, h);
        planeDef.setRegion(region);
        OmeroRenderingHandler.checkPlaneDef(
                sizeX, sizeY, planeDef);

        Assert.assertEquals(planeDef.getRegion().getWidth(), 256);
        Assert.assertEquals(planeDef.getRegion().getHeight(), 512);
    }

    @Test
    public void testCheckPlaneDefTruncate() {
        int x = 0;
        int y = 0;
        int w = 256;
        int h = 512;
        int t = 0;
        int z = 0;
        int sizeX = 100;
        int sizeY = 200;
        PlaneDef planeDef = new PlaneDef(PlaneDef.XY, t);
        planeDef.setZ(z);
        RegionDef region = new RegionDef(x, y, w, h);
        planeDef.setRegion(region);
        OmeroRenderingHandler.checkPlaneDef(
                sizeX, sizeY, planeDef);

        Assert.assertEquals(planeDef.getRegion().getWidth(), 100);
        Assert.assertEquals(planeDef.getRegion().getHeight(), 200);
    }

}
