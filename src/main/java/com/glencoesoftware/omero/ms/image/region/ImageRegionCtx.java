package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.http.HttpServerRequest;
import loci.common.Region;

public class ImageRegionCtx {

    /** Image Id*/
    private Long imageId;

    /** z - index */
    private Integer z;

    /** t - index */
    private Integer t;

    /** tile descriptor (Region) */
    private String tile;

    /** channel settings */
    private String c;

    /** Color mode (g == grey scale; c == colour) */
    private String m;

    /** Projection 'intmax' OR 'intmax|5:25' */
    private String p;

    /** Inverted axis */
    private String ia;

    /** */
    private String maps;

    ImageRegionCtx(HttpServerRequest request) {
        this.imageId = Long.parseLong(request.getParam("imageId"));
        this.z = Integer.parseInt(request.getParam("z"));
        this.t = Integer.parseInt(request.getParam("t"));
        this.tile = request.getParam("tile") == null ?
                "-" : request.getParam("tile");
        this.c = request.getParam("c") == null ?
                "-" : request.getParam("c");
        this.m = request.getParam("m") == null ?
                "-" : request.getParam("m");
    }

    private HashMap<String, Object> formatChannelInfo(
            String channelsFromRequest)
    {
        HashMap<String, Object> channels = new HashMap<String, Object>();
        String[] channelArray = channelsFromRequest.split(",", -1);
        ArrayList<Integer> active = new ArrayList<Integer>();
        ArrayList<String> colors = new ArrayList<String>();
        for (String channel : channelArray) {
            active.add(Integer.parseInt(channel.split("\\|",-1)[0]));
        }
        channels.put("active", active);
        channels.put("colors", colors);
        return channels;
    }

    public Map<String, Object> getImageRegionRaw() {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("imageId", imageId);
        data.put("t", t);
        data.put("z", z);
        data.put("tile", tile);
        data.put("c", c);
        data.put("m", m);
        return data;
    }

    public Map<String, Object> getImageRegionFormatted() {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("imageId", imageId);
        data.put("t", t);
        data.put("z", z);
        String[] tileArray = tile.split(",", -1);
        Region region = new Region(
                Integer.parseInt(tileArray[1]),
                Integer.parseInt(tileArray[2]),
                Integer.parseInt(tileArray[3]),
                Integer.parseInt(tileArray[4]));
        data.put("tile", region);
        data.put("magnification", Integer.parseInt(tileArray[0]));
        data.put("channelInfo", this.formatChannelInfo(c));
        data.put("m", m);
        data.put("region", null);
        return data;
    }
}
