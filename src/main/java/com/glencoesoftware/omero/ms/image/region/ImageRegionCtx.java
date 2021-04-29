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
import java.util.Map;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import omeis.providers.re.data.RegionDef;
import omero.constants.projection.ProjectionType;

public class ImageRegionCtx extends OmeroRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionCtx.class);

    /** Image Id */
    public Long imageId;

    /** z - index */
    public Integer z;

    /** t - index */
    public Integer t;

    /**
     * Region descriptor (tile); only X, Y, and tile width and height are used
     * at this stage and represent the <b>tile</b> offset, respecting the
     * provided tile size, rather than the pixel offset; tile width and height
     * will be 0 if not provided
     */
    public RegionDef tile;

    /** Resolution to read */
    public Integer resolution;

    /**
     * Region descriptor (region); X, Y, width, and height are used at this
     * stage and represent the pixel offset in all cases
     */
    public RegionDef region;

    /** Channel settings - handled at the Verticle level*/
    public List<Integer> channels;
    public List<Double[]> windows;
    public List<String> colors;

    /** Color mode (g == grey scale; c == rgb) */
    public String m;

    /** Codomain maps */
    public List<Map<String, Map<String, Object>>> maps;

    /** Compression quality */
    public Float compressionQuality;

    /** Projection */
    public ProjectionType projection;

    /** Projection start */
    public Integer projectionStart;

    /** Projection end */
    public Integer projectionEnd;

    /**
     * Inverted Axis
     * NOT handled at the moment - no use cases
     * */
    public Boolean invertedAxis;

    /** Rendering output format */
    public String format;

    /** Whether or not to flip horizontally */
    public boolean flipHorizontal;

    /** Whether or not to flip vertically */
    public boolean flipVertical;

    /**
     * Constructor for jackson to decode the object from string
     */
    ImageRegionCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     * @param omeroSessionKey OMERO session key.
     */
    ImageRegionCtx(MultiMap params, String omeroSessionKey) {
        try {
            this.omeroSessionKey = omeroSessionKey;
            assignParams(params);
        } catch (Exception e) {
            log.error("Error creating ImageRegionCtx", e);
            throw e;
        }
    }

    ImageRegionCtx(JsonObject params, String omeroSessionKey) {
        try {
            this.omeroSessionKey = omeroSessionKey;
            assignParams(params);
        } catch (Exception e) {
            log.error("Error creating ImageRegionCtx", e);
            throw e;
        }
    }

    public void assignParams(JsonObject params) throws IllegalArgumentException {
        getImageIdFromString(getCheckedJsonParam(params, "imageId"));
        z = getIntegerFromString(getCheckedJsonParam(params, "theZ"));
        t = getIntegerFromString(getCheckedJsonParam(params, "theT"));
        getTileFromString(params.getString("tile"));
        getRegionFromString(params.getString("region"));
        getChannelInfoFromString(params.getString("c"));
        getColorModelFromString(params.getString("m"));
        getCompressionQualityFromString(params.getString("q"));
        getInvertedAxisFromString(params.getString("ia"));
        getProjectionFromString(params.getString("p"));
        String maps = params.getString("maps");
        String flip = Optional.ofNullable(params.getString("flip"))
                .orElse("").toLowerCase();
        flipHorizontal = flip.contains("h");
        flipVertical = flip.contains("v");
        if (maps != null) {
            this.maps = Json.decodeValue(maps, List.class);
        }
        format = Optional.ofNullable(params.getString("format")).orElse("jpeg");

        log.debug(
                "{}, z: {}, t: {}, tile: {}, c: [{}, {}, {}], m: {}, " +
                "format: {}", imageId, z, t, tile, channels, windows, colors,
                m, format);
    }

    public void assignParams(MultiMap params) throws IllegalArgumentException {
        getImageIdFromString(getCheckedParam(params, "imageId"));
        z = getIntegerFromString(getCheckedParam(params, "theZ"));
        t = getIntegerFromString(getCheckedParam(params, "theT"));
        getTileFromString(params.get("tile"));
        getRegionFromString(params.get("region"));
        getChannelInfoFromString(params.get("c"));
        getColorModelFromString(params.get("m"));
        getCompressionQualityFromString(params.get("q"));
        getInvertedAxisFromString(params.get("ia"));
        getProjectionFromString(params.get("p"));
        String maps = params.get("maps");
        String flip = Optional.ofNullable(params.get("flip"))
                .orElse("").toLowerCase();
        flipHorizontal = flip.contains("h");
        flipVertical = flip.contains("v");
        if (maps != null) {
            this.maps = Json.decodeValue(maps, List.class);
        }
        format = Optional.ofNullable(params.get("format")).orElse("jpeg");

        log.debug(
                "{}, z: {}, t: {}, tile: {}, c: [{}, {}, {}], m: {}, " +
                "format: {}", imageId, z, t, tile, channels, windows, colors,
                m, format);
    }

    private String getCheckedJsonParam(JsonObject params, String key)
            throws IllegalArgumentException {
            String value = params.getString(key);
            if (null == value) {
                throw new IllegalArgumentException("Missing parameter '"
                    + key + "'");
            }
            return value;
        }

    private String getCheckedParam(MultiMap params, String key)
        throws IllegalArgumentException {
        String value = params.get(key);
        if (null == value) {
            throw new IllegalArgumentException("Missing parameter '"
                + key + "'");
        }
        return value;
    }

    /**
     * Parse a string to Long and set ast the image ID.
     * @param imageIdString string
     */
    private void getImageIdFromString(String imageIdString)
        throws IllegalArgumentException{
        try {
            imageId = Long.parseLong(imageIdString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Incorrect format for "
                + "imageid parameter '" + imageIdString + "'");
        }
    }

    /**
     * Parse a string to Integer and return it
     * @param imageIdString string
     */
    private Integer getIntegerFromString(String intString)
        throws IllegalArgumentException{
        Integer i = null;
        try {
            i = Integer.parseInt(intString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Incorrect format for "
                + "parameter value '" + intString + "'");
        }
        return i;
    }

    /**
     * Parse a string to RegionDef and Int describing tile and resolution.
     * @param tileString string describing the tile to render:
     * "1,1,0,1024,1024"
     */
    private void getTileFromString(String tileString) {
        if (tileString == null) {
            return;
        }
        String[] tileArray = tileString.split(",", -1);
        tile = new RegionDef();
        tile.setX(Integer.parseInt(tileArray[1]));
        tile.setY(Integer.parseInt(tileArray[2]));
        if (tileArray.length == 5) {
            tile.setWidth(Integer.parseInt(tileArray[3]));
            tile.setHeight(Integer.parseInt(tileArray[4]));
        }
        resolution = Integer.parseInt(tileArray[0]);
    }

    /**
     * Parse a string to RegionDef.
     * @param regionString string describing the region to render:
     * "0,0,1024,1024"
     */
    private void getRegionFromString(String regionString) {
        if (regionString == null) {
            return;
        }
        String[] regionSplit = regionString.split(",", -1);
        if (regionSplit.length != 4) {
            throw new IllegalArgumentException("Region string format incorrect. "
                + "Should be 'x,y,w,h'");
        }
        try {
            region = new RegionDef(
                Integer.parseInt(regionSplit[0]),
                Integer.parseInt(regionSplit[1]),
                Integer.parseInt(regionSplit[2]),
                Integer.parseInt(regionSplit[3])
            );
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Improper number formatting "
                + "in region string " + regionString);
        }
    }

    /**
     * Parses a string to channel rendering settings.
     * Populates channels, windows and colors lists.
     * @param channelInfo string describing the channel rendering settings:
     * "-1|0:65535$0000FF,2|1755:51199$00FF00,3|3218:26623$FF0000"
     */
    private void getChannelInfoFromString(String channelInfo) {
        if (channelInfo == null) {
            return;
        }
        String[] channelArray = channelInfo.split(",", -1);
        channels = new ArrayList<Integer>();
        windows = new ArrayList<Double[]>();
        colors = new ArrayList<String>();
        for (String channel : channelArray) {
            try {
                // chan  1|12:1386r$0000FF
                // temp ['1', '12:1386r$0000FF']
                String[] temp = channel.split("\\|", 2);
                String active = temp[0];
                String color = null;
                Double[] range = new Double[2];
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
                        range[0] = Double.parseDouble(rangeStr[0]);
                        range[1] = Double.parseDouble(rangeStr[1]);
                    }
                }
                colors.add(color);
                windows.add(range);
                log.debug("Adding channel: {}, color: {}, window: {}",
                        active, color, window);
            } catch (Exception e)  {
                throw new IllegalArgumentException("Failed to parse channel '"
                    + channel + "'");
            }
        }
    }

    /**
     * Parses color model input to the string accepted by the rendering engine.
     * @param colorModel string describing color model:
     * "g" for greyscale and "c" for rgb.
     */
    private void getColorModelFromString(String colorModel) {
        if ("g".equals(colorModel)) {
            m = "greyscale";
        } else if ("c".equals(colorModel)) {
            m = "rgb";
        } else {
            m = null;
        }
    }

    /**
     * Parses string to Float and sets it as compressionQuality.
     * @param quality accepted values: [0, 1]
     */
    private void getCompressionQualityFromString(String quality) {
        compressionQuality = quality == null? null : Float.parseFloat(quality);
    }

    /**
     * Parses string to boolean and sets it as inverted axis.
     * @param iaString accepted values: 0 - False, 1 - True
     */
    private void getInvertedAxisFromString(String iaString) {
        invertedAxis = iaString == null? null : Boolean.parseBoolean(iaString);
    }

    /**
     * Parses string to projection enumeration and sets projection start and
     * end.  Accepted modes of projection include:
     * <ul>
     *   <li><code>normal</code></li>
     *   <li><code>intmax</code></li>
     *   <li><code>intmean</code></li>
     *   <li><code>intsum</code></li>
     * </ul>
     * @param projection accepted form <code>[mode]|[start]:[end]</code>
     */
    private void getProjectionFromString(String projection) {
        if (projection == null) {
            return;
        }

        String[] parts = projection.split("\\|", -1);
        switch(parts[0]) {
            case "intmax": {
                this.projection = ProjectionType.MAXIMUMINTENSITY;
                break;
            }
            case "intmean": {
                this.projection = ProjectionType.MEANINTENSITY;
                break;
            }
            case "intsum": {
                this.projection = ProjectionType.SUMINTENSITY;
                break;
            }
        }

        if (parts.length != 2) {
            return;
        }

        parts = parts[1].split(":");
        try {
            projectionStart = Integer.parseInt(parts[0]);
            projectionEnd = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            // Ignore
        }
    }
}
