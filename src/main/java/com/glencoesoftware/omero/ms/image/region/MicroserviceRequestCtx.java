/*
 * Copyright (C) 2023 Glencoe Software, Inc. All rights reserved.
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

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;

/**
 * Abstract class to allow microservice request contexts to share
 * helper functions
 * @author kevin
 *
 */
abstract class MicroserviceRequestCtx extends OmeroRequestCtx {

    protected String getCheckedParam(MultiMap params, String key)
        throws IllegalArgumentException {
        String value = params.get(key);
        if (null == value) {
            throw new IllegalArgumentException("Missing parameter '"
                + key + "'");
        }
        return value;
    }

    /**
     * Parse a string to Long and return.
     * @param imageIdString string
     */
    protected Long getImageIdFromString(String imageIdString)
        throws IllegalArgumentException{
        try {
            return Long.parseLong(imageIdString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Incorrect format for "
                + "imageId parameter '" + imageIdString + "'");
        }
    }

    /**
     * Parse a string to Integer and return it
     * @param imageIdString string
     */
    protected Integer getIntegerFromString(String intString)
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
}
