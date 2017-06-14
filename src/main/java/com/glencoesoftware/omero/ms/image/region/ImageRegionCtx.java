package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import io.vertx.core.MultiMap;
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

    /** Color mode (g == grey scale; c == rgb) */
    private String m;

    /** Maps
     * NOT handled at the moment supported from 5.3.0 */
    private String maps;

    /** Compression quality */
    private String compressionQuality;

    /** Projection 'intmax' OR 'intmax|5:25'
     * NOT handled at the moment - does not look like it's supported
     * for renderImageRegion: https://github.com/openmicroscopy/openmicroscopy/blob/be40a59300bb73a22b72eac00dd24b2aa54e4768/components/tools/OmeroPy/src/omero/gateway/__init__.py#L8758
     * vs. renderImage: https://github.com/openmicroscopy/openmicroscopy/blob/be40a59300bb73a22b72eac00dd24b2aa54e4768/components/tools/OmeroPy/src/omero/gateway/__init__.py#L8837
     * */
    private String projection;

    /** Inverted Axis
     *  NOT handled at the moment - no use cases*/
    private Boolean invertedAxis;

    ImageRegionCtx(MultiMap params) {
        this.imageId = Long.parseLong(params.get("imageId"));
        this.z = Integer.parseInt(params.get("z"));
        this.t = Integer.parseInt(params.get("t"));
        this.tile = params.get("tile");
        this.region = params.get("region");
        this.c = params.get("c");
        this.m = params.get("m");
        this.compressionQuality = params.get("q");
        this.projection = params.get("p");
        this.maps = params.get("maps");
        this.invertedAxis = Boolean.parseBoolean(params.get("ia"));
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
        String model = null;
        if (this.m != null && this.m.equals("g")) {
            model = "greyscale";
        } else if (this.m != null && this.m.equals("c")) {
            model = "rgb";
        }
        data.put("m", model);
        if (this.compressionQuality != null) {
            data.put("compressionQuality",
                     Float.parseFloat(this.compressionQuality));
        } else {
            data.put("compressionQuality", this.compressionQuality);
        }
        data.put("invertedAxis", this.invertedAxis);
        data.put("projection", this.projection);
        data.put("maps", this.maps);
        return data;
    }
}
