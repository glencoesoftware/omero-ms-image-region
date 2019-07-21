package com.glencoesoftware.omero.ms.image.region;

import brave.ScopedSpan;
import brave.Tracer;
import brave.http.HttpTracing;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

public class ImageRegionTracingContextHandler implements Handler<RoutingContext> {

    final HttpTracing m_httpTracing;

    ImageRegionTracingContextHandler(HttpTracing httpTracing) {
        m_httpTracing = httpTracing;
    }

    @Override
    public void handle(RoutingContext context) {
        Tracer tracer = m_httpTracing.tracing().tracer();
        ScopedSpan span = tracer.startScopedSpan("image_region");
        TracingEndHandler handler = new TracingEndHandler(context, span);
        context.addHeadersEndHandler(handler);
        context.next();
    }

}

final class TracingEndHandler implements Handler<Void> {

    private ScopedSpan m_span;
    private RoutingContext m_context;

    TracingEndHandler(RoutingContext context, ScopedSpan span) {
        m_context = context;
        m_span = span;
    }

    @Override
    public void handle(Void event) {
        try {
            String sessionId = m_context.get("omero.session_key");
            String requestId = m_context.get("omero_ms.request_id");
            m_span.tag("sessionid", sessionId);
            m_span.tag("request_id", requestId);
        } finally {
            m_span.finish();
        }
    }
}