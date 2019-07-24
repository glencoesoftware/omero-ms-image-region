/*
 * Copyright (C) 2019 Glencoe Software, Inc. All rights reserved.
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

import brave.ScopedSpan;
import brave.Tracer;
import brave.http.HttpTracing;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

public class ImageRegionTracingContextHandler
        implements Handler<RoutingContext> {

    private final Tracer tracer;

    ImageRegionTracingContextHandler(HttpTracing httpTracing) {
        tracer = httpTracing.tracing().tracer();
    }

    @Override
    public void handle(RoutingContext context) {
        ScopedSpan span = tracer.startScopedSpan("vertx.http_request");
        HttpServerRequest request = context.request();
        span.tag("http.method", request.rawMethod());
        span.tag("http.path", request.path());
        span.tag("http.params", request.params().toString());
        TracingHandler handler = new TracingHandler(context, span);
        context.put(TracingHandler.class.getName(), handler);
        context.addHeadersEndHandler(handler);
        context.next();
    }

}

final class TracingHandler implements Handler<Void> {

    final ScopedSpan span;

    final RoutingContext context;

    TracingHandler(RoutingContext context, ScopedSpan span) {
        this.context = context;
        this.span = span;
    }

    @Override
    public void handle(Void event) {
        try {
            String sessionId = context.get("omero.session_key");
            String requestId = context.get("omero_ms.request_id");
            span.tag("sessionid", sessionId);
            span.tag("request_id", requestId);
        } finally {
            span.finish();
        }
    }
}
