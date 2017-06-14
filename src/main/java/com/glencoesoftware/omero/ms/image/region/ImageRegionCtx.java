/*
 * Copyright (C) 2017 Glencoe Software, Inc. All rights reserved.
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
import java.util.HashMap;
import java.util.List;
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

    /** Maps. <b>Not</b> handled at the moment. Supported from 5.3.0 */
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

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     */
    ImageRegionCtx(MultiMap params) {
        imageId = Long.parseLong(params.get("imageId"));
        z = Integer.parseInt(params.get("z"));
        t = Integer.parseInt(params.get("t"));
        tile = params.get("tile");
        region = params.get("region");
        c = params.get("c");
        m = params.get("m");
        compressionQuality = params.get("q");
        projection = params.get("p");
        maps = params.get("maps");
        invertedAxis = Boolean.parseBoolean(params.get("ia"));
        log.debug("Image:{}, z: {}, t: {}, tile: {}, c: {}, m: {}",
                this.imageId, this.z, this.t,
                this.tile, this.c, this.m);
    }

    private Map<String, Object> formatChannelInfo(String channelsFromRequest) {
        Map<String, Object> channels = new HashMap<String, Object>();
        String[] channelArray = channelsFromRequest.split(",", -1);
        List<Integer> activeChannels = new ArrayList<Integer>();
        List<String> colors = new ArrayList<String>();
        List<Integer[]> windows = new ArrayList<Integer[]>();
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
        data.put("region", region);
        data.put("c", c);
        data.put("m", m);
        data.put("compressionQuality", compressionQuality);
        data.put("invertedAxis", invertedAxis);
        data.put("projection", projection);
        data.put("maps", maps);
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
            String[] tileArray = tile.split(",", -1);
            JsonArray tileCoor = new JsonArray();
            tileCoor.add(Integer.parseInt(tileArray[1]));
            tileCoor.add(Integer.parseInt(tileArray[2]));
            data.put("tile", tileCoor);
            data.put("resolution", Integer.parseInt(tileArray[0]));
        } else if (this.region != null) {
            String[] regionSplit = region.split(",", -1);
            JsonArray regionCoor = new JsonArray();
            regionCoor.add(Integer.parseInt(regionSplit[0]));
            regionCoor.add(Integer.parseInt(regionSplit[1]));
            regionCoor.add(Integer.parseInt(regionSplit[2]));
            regionCoor.add(Integer.parseInt(regionSplit[3]));
            data.put("region", regionCoor);
        }
        data.put("channelInfo", formatChannelInfo(c));
        String model = null;
        if (m != null && m.equals("g")) {
            model = "greyscale";
        } else if (m != null && m.equals("c")) {
            model = "rgb";
        }
        data.put("m", model);
        if (compressionQuality != null) {
            data.put("compressionQuality",
                     Float.parseFloat(compressionQuality));
        } else {
            data.put("compressionQuality", compressionQuality);
        }
        data.put("invertedAxis", invertedAxis);
        data.put("projection", projection);
        data.put("maps", maps);
        return data;
    }
}
