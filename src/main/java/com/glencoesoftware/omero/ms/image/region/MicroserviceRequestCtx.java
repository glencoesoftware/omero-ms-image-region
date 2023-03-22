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
     * Parse a string to Long and set ast the image ID.
     * @param imageIdString string
     */
    protected Long getImageIdFromString(String imageIdString)
        throws IllegalArgumentException{
        try {
            return Long.parseLong(imageIdString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Incorrect format for "
                + "imageid parameter '" + imageIdString + "'");
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
