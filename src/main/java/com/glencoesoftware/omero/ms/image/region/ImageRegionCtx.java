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
import java.util.List;
import org.slf4j.LoggerFactory;
import io.vertx.core.MultiMap;;

public class ImageRegionCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionCtx.class);

    /** Image Id*/
    public Long imageId;

    /** z - index */
    public Integer z;

    /** t - index */
    public Integer t;

    /** tile descriptor (Region) */
    public List<Integer> tile;

    /** resolution to read */
    public Integer resolution;

    /** tile descriptor (Region) */
    public List<Integer> region;

    /** channel settings - handled at Verticle level*/
    public List<Integer> channels;
    public List<Integer[]> windows;
    public List<String> colors;

    /** Color mode (g == grey scale; c == rgb) */
    public String m;

    /** Maps. <b>Not</b> handled at the moment. Supported from 5.3.0 */
    public String maps;

    /** Compression quality */
    public Float compressionQuality;

    /** Projection 'intmax' OR 'intmax|5:25'
     * NOT handled at the moment - does not look like it's supported
     * for renderImageRegion: https://github.com/openmicroscopy/openmicroscopy/blob/be40a59300bb73a22b72eac00dd24b2aa54e4768/components/tools/OmeroPy/src/omero/gateway/__init__.py#L8758
     * vs. renderImage: https://github.com/openmicroscopy/openmicroscopy/blob/be40a59300bb73a22b72eac00dd24b2aa54e4768/components/tools/OmeroPy/src/omero/gateway/__init__.py#L8837
     * */
    public String projection;

    /** Inverted Axis
     *  NOT handled at the moment - no use cases*/
    public Boolean invertedAxis;

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     */
    ImageRegionCtx(MultiMap params) {
        imageId = Long.parseLong(params.get("imageId"));
        z = Integer.parseInt(params.get("z"));
        t = Integer.parseInt(params.get("t"));
        getTileFromString(params.get("tile"));
        getRegionFromString(params.get("region"));
        getChannelInfoFromString(params.get("c"));
        getColorModelFromString(params.get("m"));
        getCompressionQualityFromString(params.get("q"));
        getInvertedAxisFromString(params.get("ia"));
        projection = params.get("p");
        maps = params.get("maps");
        log.debug("{}, z: {}, t: {}, tile: {}, c: [{}, {}, {}], m: {}",
                imageId, z, t, tile, channels, windows, colors, m);
    }

    private void getTileFromString(String tileString) {
        if (tileString == null) {
            return;
        }
        String[] tileArray = tileString.split(",", -1);
        tile = new ArrayList<Integer>();
        tile.add(Integer.parseInt(tileArray[1]));
        tile.add(Integer.parseInt(tileArray[2]));
        resolution = Integer.parseInt(tileArray[0]);
    }

    private void getRegionFromString(String regionString) {
        if (regionString == null) {
            return;
        }
        String[] regionSplit = regionString.split(",", -1);
        region = new ArrayList<Integer>();
        region.add(Integer.parseInt(regionSplit[0]));
        region.add(Integer.parseInt(regionSplit[1]));
        region.add(Integer.parseInt(regionSplit[2]));
        region.add(Integer.parseInt(regionSplit[3]));
    }

    private void getChannelInfoFromString(String channelInfo) {
        if (channelInfo == null) {
            return;
        }
        String[] channelArray = channelInfo.split(",", -1);
        channels = new ArrayList<Integer>();
        windows = new ArrayList<Integer[]>();
        colors = new ArrayList<String>();
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
            channels.add(Integer.parseInt(active));
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
    }

    private void getColorModelFromString(String colorModel) {
        if (colorModel != null && colorModel.equals("g")) {
            m = "greyscale";
        } else if (colorModel != null && colorModel.equals("c")) {
            m = "rgb";
        } else {
            m = null;
        }
    }

    private void getCompressionQualityFromString(String quality) {
        if (quality == null) {
            compressionQuality = null;
            return;
        }
        compressionQuality = Float.parseFloat(quality);
    }

    private void getInvertedAxisFromString(String iaString) {
        if (iaString == null) {
            invertedAxis = null;
            return;
        }
        invertedAxis = Boolean.parseBoolean(iaString);
    }
}
