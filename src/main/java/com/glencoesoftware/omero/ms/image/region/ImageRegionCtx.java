package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import io.vertx.core.http.HttpServerRequest;
import loci.common.Region;

public class ImageRegionCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionCtx.class);

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
        log.debug("imageId: {}, z: {}, t: {}, tile: {}, c: {}, m: {}",
                this.imageId, this.z, this.t,
                this.tile, this.c, this.m);
    }

    private HashMap<String, Object> formatChannelInfo(
            String channelsFromRequest)
    {
        HashMap<String, Object> channels = new HashMap<String, Object>();
        String[] channelArray = channelsFromRequest.split(",", -1);
        ArrayList<Integer> activeChannels = new ArrayList<Integer>();
        ArrayList<String> colors = new ArrayList<String>();
        ArrayList<Integer[]> windows = new ArrayList<Integer[]>();
        for (String channel : channelArray) {
            // chan  1|12:1386r$0000FF
            // temp ['1', '12:1386r$0000FF']
            String[] temp = channel.split("\\|", 2);
            String active = temp[0];
            String color = null;
            Integer[] range = new Integer[2];
            String window = null;
            // temp = '1'
            // Not normally used...
            if (active.indexOf("$") >= 0) {
                String[] split = active.split("\\$", -1);
                active = split[0];
                color = split[1];
            }
            activeChannels.add(Integer.parseInt(active));
            if (temp.length > 1) {
                if (temp[1].indexOf("$") >= 0) {
                    window = temp[1].split("\\$")[0];
                    color = temp[1].split("\\$")[1];
                }
                String[] rangeStr = window.split(":");
                if (rangeStr.length > 1) {
                    range[0] = Integer.parseInt(rangeStr[0]);
                    range[1] = Integer.parseInt(rangeStr[1]);
                }
            }
            colors.add(color);
            windows.add(range);
            log.debug("Adding channel: {}, color: {}, window: {}",
                    active, color, window);
        }
        channels.put("active", activeChannels);
        channels.put("colors", colors);
        channels.put("windows", windows);
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
