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

import org.slf4j.LoggerFactory;

import com.glencoesoftware.omero.ms.core.OmeroRequest;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class ImageRegionVerticle extends AbstractVerticle {

	private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionVerticle.class);

    public static final String RENDER_IMAGE_REGION_EVENT =
            "omero.render_image_region";

    /** OMERO server host */
    private final String host;

    /** OMERO server port */
    private final int port;

    /**
     * Default constructor.
     * @param host OMERO server host.
     * @param port OMERO server port.
     */
    public ImageRegionVerticle(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        log.info("Starting verticle");

        vertx.eventBus().<String>consumer(
                RENDER_IMAGE_REGION_EVENT, this::renderImageRegion);
    }

    /**
     * Render image region event handler.
     * Responds with a <code>image/jpeg</code>
     * body on success or a failure.
     * @param message JSON encoded event data. Required keys are
     * <code>omeroSessionKey</code> (String), <code>longestSide</code>
     * (Integer), and <code>imageId</code> (Long).
     */
    private void renderImageRegion(Message<String> message) {
        JsonObject data = new JsonObject(message.body());
        log.debug(
            "Render image region request with data: {}", data);
    }
}
