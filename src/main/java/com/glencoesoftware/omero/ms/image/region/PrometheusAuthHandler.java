/*
 * Copyright (C) 2018 Glencoe Software, Inc. All rights reserved.
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

import java.util.Base64;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.core.http.HttpServerRequest;

class PrometheusAuthHandler implements Handler<RoutingContext> {
    private String correct_username;
    private String correct_password;
    PrometheusAuthHandler (String username, String password) {
        correct_username = username;
        correct_password = password;
    }

    public void handle(RoutingContext r){
        HttpServerRequest request = r.request();
        String header = request.getHeader("Authorization");
        if (header == null
            || header.isEmpty()
            || header.indexOf("Basic") == -1) {
            r.response()
            .setStatusCode(401)
            .putHeader("WWW-Authenticate",
                "Basic realm=\"Prometheus metrics\"")
            .end("Missing Authentication");
        }
        String base64Credentials =
            header.substring("Basic".length()).trim();
        String authInfo =
            new String(Base64.getDecoder().decode(base64Credentials));
        String username = authInfo.split(":",2)[0];
        String password = authInfo.split(":",2)[0];
        if (!username.equals(correct_username)
            || !password.equals(correct_password)) {
            r.response().setStatusCode(403).end("Not authenticated");
        }
        else {
            r.next();
        }
    }
}
