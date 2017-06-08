package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;

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

    /** tile descriptor (Region) */
    private String region;

    /** channel settings - handled at Verticle level*/
    private String c;

    /** Color mode (g == grey scale; c == colour)
     * NOT handled at the moment
     * */
    private String m;

    /** Maps
     * NOT handled at the moment */
    private String maps;

    /** Compression quality 
     * NOT handled at the moment
     * */
    private Float compressionQuality;

    /** Projection 'intmax' OR 'intmax|5:25'
     * NOT handled at the moment
     * */
    private String projection;

    /** Inverted Axis
     *  NOT handled at the moment*/
    private Boolean invertedAxis;

    ImageRegionCtx(HttpServerRequest request) {
        this.imageId = Long.parseLong(request.getParam("imageId"));
        this.z = Integer.parseInt(request.getParam("z"));
        this.t = Integer.parseInt(request.getParam("t"));
        this.tile = request.getParam("tile") == null ?
                null : request.getParam("tile");
        this.region = request.getParam("region") == null ?
                null : request.getParam("region");
        this.c = request.getParam("c") == null ?
                null : request.getParam("c");
        this.m = request.getParam("m") == null ?
                null : request.getParam("m");
        this.compressionQuality = request.getParam("q") == null ?
                null : Float.parseFloat(request.getParam("q"));
        this.projection = request.getParam("p") == null ?
                null : request.getParam("p");
        this.maps = request.getParam("maps") == null ?
                null : request.getParam("maps");
        this.invertedAxis = request.getParam("ia") == null ?
                null : Boolean.parseBoolean(request.getParam("ia"));
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
        data.put("t", this.t);
        data.put("z", this.z);
        data.put("tile", this.tile);
        data.put("region", this.region);
        data.put("c", this.c);
        data.put("m", this.m);
        data.put("compressionQuality", this.compressionQuality);
        data.put("invertedAxis", this.invertedAxis);
        data.put("projection", this.projection);
        data.put("maps", this.maps);
        return data;
    }

    public Map<String, Object> getImageRegionFormatted() {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("imageId", imageId);
        data.put("t", t);
        data.put("z", z);
        data.put("tile", null);
        data.put("resolution", null);
        data.put("region", null);
        if (this.tile != null) {
            String[] tileArray = this.tile.split(",", -1);
            JsonArray tileCoor = new JsonArray();
            tileCoor.add(Integer.parseInt(tileArray[1]));
            tileCoor.add(Integer.parseInt(tileArray[2]));
            data.put("tile", tileCoor);
            data.put("resolution", Integer.parseInt(tileArray[0]));
        } else if (this.region != null) {
            String[] regionSplit = this.region.split(",", -1);
            JsonArray regionCoor = new JsonArray();
            regionCoor.add(Integer.parseInt(regionSplit[0]));
            regionCoor.add(Integer.parseInt(regionSplit[1]));
            regionCoor.add(Integer.parseInt(regionSplit[2]));
            regionCoor.add(Integer.parseInt(regionSplit[3]));
            data.put("region", regionCoor);
        }
        data.put("channelInfo", this.formatChannelInfo(c));
        data.put("m", m);
        data.put("compressionQuality", this.compressionQuality);
        data.put("invertedAxis", this.invertedAxis);
        data.put("projection", this.projection);
        data.put("maps", this.maps);
        return data;
    }
}
