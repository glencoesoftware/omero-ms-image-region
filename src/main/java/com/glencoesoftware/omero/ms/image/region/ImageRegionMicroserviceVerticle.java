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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.glencoesoftware.omero.ms.core.PrometheusSpanHandler;
import com.glencoesoftware.omero.ms.core.OmeroHttpTracingHandler;
import com.glencoesoftware.omero.ms.core.LogSpanReporter;
import com.glencoesoftware.omero.ms.core.OmeroVerticleFactory;
import com.glencoesoftware.omero.ms.core.OmeroWebJDBCSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebRedisSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionRequestHandler;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionStore;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CookieHandler;

import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;
import brave.Tracing;
import brave.http.HttpTracing;
import brave.sampler.Sampler;
import io.prometheus.client.vertx.MetricsHandler;
import io.prometheus.jmx.BuildInfoCollector;
import io.prometheus.jmx.JmxCollector;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * Main entry point for the OMERO image region Vert.x microservice server.
 * @author Chris Allan <callan@glencoesoftware.com>
 * @author Emil Rozbicki <emil@glencoesoftware.com>
 *
 */
public class ImageRegionMicroserviceVerticle extends AbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionMicroserviceVerticle.class);

    private static final String JMX_CONFIG =
            "---\n"
            + "startDelaySeconds: 0\n";

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /** OMERO.web session store */
    private OmeroWebSessionStore sessionStore;

    /** VerticleFactory */
    private OmeroVerticleFactory verticleFactory;

    /** Default number of workers (core count * 2) */
    private int DEFAULT_WORKER_POOL_SIZE;

    /** The string which will be used as Cache-Control header in responses */
    private String cacheControlHeader;

    /** Zipkin HTTP Tracing*/
    private HttpTracing httpTracing;

    private OkHttpSender sender;

    private AsyncReporter<Span> spanReporter;

    private Tracing tracing;


    /**
     * Entry point method which starts the server event loop and initializes
     * our current OMERO.web session store.
     */
    @Override
    public void start(Promise<Void> promise) {
        log.info("Starting verticle");

        DEFAULT_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

        ConfigStoreOptions store = new ConfigStoreOptions()
                .setType("file")
                .setFormat("yaml")
                .setConfig(new JsonObject()
                        .put("path", "conf/config.yaml")
                )
                .setOptional(true);
        ConfigRetriever retriever = ConfigRetriever.create(
                vertx, new ConfigRetrieverOptions()
                        .setIncludeDefaultStores(true)
                        .addStore(store));
        retriever.getConfig(new Handler<AsyncResult<JsonObject>>() {
            @Override
            public void handle(AsyncResult<JsonObject> ar) {
                try {
                    deploy(ar.result(), promise);
                } catch (Exception e) {
                    promise.fail(e);
                }
            }
        });
    }

    /**
     * Deploys our verticles and performs general setup that depends on
     * configuration.
     * @param config Current configuration
     */
    public void deploy(JsonObject config, Promise<Void> promise) {
        log.info("Deploying verticle");

        // Set OMERO.server configuration options using system properties
        JsonObject omeroServer = config.getJsonObject("omero.server");
        if (omeroServer == null) {
            throw new IllegalArgumentException(
                    "'omero.server' block missing from configuration");
        }
        for (Map.Entry<String, Object> entry : omeroServer) {
            System.setProperty(entry.getKey(), (String) entry.getValue());
        }

        context = new ClassPathXmlApplicationContext(
                "classpath:ome/config.xml",
                "classpath:ome/services/datalayer.xml",
                "classpath*:beanRefContext.xml",
                "classpath*:service-ms.core.PixelsService.xml");

        JsonObject httpTracingConfig =
                config.getJsonObject("http-tracing", new JsonObject());
        Boolean tracingEnabled =
                httpTracingConfig.getBoolean("enabled", false);
        if (tracingEnabled) {
            String zipkinUrl = httpTracingConfig.getString("zipkin-url");
            log.info("Tracing enabled: {}", zipkinUrl);
            sender = OkHttpSender.create(zipkinUrl);
            spanReporter = AsyncReporter.create(sender);
            PrometheusSpanHandler prometheusSpanHandler = new PrometheusSpanHandler();
            tracing = Tracing.newBuilder()
                .sampler(Sampler.ALWAYS_SAMPLE)
                .localServiceName("omero-ms-image-region")
                .addFinishedSpanHandler(prometheusSpanHandler)
                .spanReporter(spanReporter)
                .build();
        } else {
            log.info("Tracing disabled");
            PrometheusSpanHandler prometheusSpanHandler = new PrometheusSpanHandler();
            spanReporter = new LogSpanReporter();
            tracing = Tracing.newBuilder()
                    .sampler(Sampler.ALWAYS_SAMPLE)
                    .localServiceName("omero-ms-image-region")
                    .addFinishedSpanHandler(prometheusSpanHandler)
                    .spanReporter(spanReporter)
                    .build();
        }
        httpTracing = HttpTracing.newBuilder(tracing).build();

        JsonObject jmxMetricsConfig =
                config.getJsonObject("jmx-metrics", new JsonObject());
        Boolean jmxMetricsEnabled =
                jmxMetricsConfig.getBoolean("enabled", false);
        if (jmxMetricsEnabled) {
            log.info("JMX Metrics Enabled");
            new BuildInfoCollector().register();
            try {
                new JmxCollector(JMX_CONFIG).register();
                DefaultExports.initialize();
            } catch (Exception e) {
                log.error("Error setting up JMX Metrics", e);
            }
        }
        else {
            log.info("JMX Metrics NOT Enabled");
        }


        verticleFactory = (OmeroVerticleFactory)
                context.getBean("omero-ms-verticlefactory");
        vertx.registerVerticleFactory(verticleFactory);
        // Deploy our dependency verticles
        int workerPoolSize = Optional.ofNullable(
            config.getInteger("worker_pool_size")
        ).orElse(DEFAULT_WORKER_POOL_SIZE);
        vertx.deployVerticle("omero:omero-ms-redis-cache-verticle",
                new DeploymentOptions().setConfig(config));
        vertx.deployVerticle("omero:omero-ms-image-region-verticle",
                new DeploymentOptions()
                .setWorker(true)
                .setInstances(workerPoolSize)
                .setWorkerPoolName("render-image-region-pool")
                .setWorkerPoolSize(workerPoolSize)
                .setConfig(config));
        vertx.deployVerticle("omero:omero-ms-shape-mask-verticle",
                new DeploymentOptions()
                    .setWorker(true)
                    .setInstances(workerPoolSize)
                    .setWorkerPoolName("render-shape-mask-pool")
                    .setWorkerPoolSize(workerPoolSize)
                    .setConfig(config));

        HttpServerOptions options = new HttpServerOptions();
        options.setMaxInitialLineLength(config.getInteger(
            "max-initial-line-length",
            HttpServerOptions.DEFAULT_MAX_INITIAL_LINE_LENGTH)
        );
        options.setMaxHeaderSize(config.getInteger(
            "max-header-size",
            HttpServerOptions.DEFAULT_MAX_HEADER_SIZE)
        );
        options.setMaxChunkSize(config.getInteger(
            "max-chunk-size",
            HttpServerOptions.DEFAULT_MAX_CHUNK_SIZE)
        );

        HttpServer server = vertx.createHttpServer(options);
        Router router = Router.router(vertx);

        router.get("/metrics")
        .order(-2)
        .handler(new MetricsHandler());

        List<String> tags = new ArrayList<String>();
        tags.add("omero.session_key");

        Handler<RoutingContext> routingContextHandler =
                new OmeroHttpTracingHandler(httpTracing, tags);
        // Set up HttpTracing Routing
        router.route()
            .order(-1) // applies before other routes
            .handler(routingContextHandler)
            .failureHandler(routingContextHandler);


        cacheControlHeader = config.getString("cache-control-header", "");

        // Get ImageRegion Microservice Information
        router.options().handler(this::getMicroserviceDetails);

        // Cookie handler so we can pick up the OMERO.web session
        router.route().handler(CookieHandler.create());

        // OMERO session handler which picks up the session key from the
        // OMERO.web session and joins it.
        JsonObject sessionStoreConfig = config.getJsonObject("session-store");
        if (sessionStoreConfig == null) {
            throw new IllegalArgumentException(
                    "'session-store' block missing from configuration");
        }
        String sessionStoreType = sessionStoreConfig.getString("type");
        String sessionStoreUri = sessionStoreConfig.getString("uri");
        if (sessionStoreType.equals("redis")) {
            sessionStore = new OmeroWebRedisSessionStore(sessionStoreUri);
        } else if (sessionStoreType.equals("postgres")) {
            sessionStore = new OmeroWebJDBCSessionStore(
                sessionStoreUri,
                vertx);
        } else {
            throw new IllegalArgumentException(
                "Missing/invalid value for 'session-store.type' in config");
        }
        router.route().handler(
                new OmeroWebSessionRequestHandler(config, sessionStore));

        // ImageRegion request handlers
        router.get(
                "/webgateway/render_image_region/:imageId/:theZ/:theT*")
            .handler(this::renderImageRegion);
        router.get(
                "/webgateway/render_image/:imageId/:theZ/:theT*")
            .handler(this::renderImageRegion);
        router.get(
                "/webclient/render_image_region/:imageId/:theZ/:theT*")
            .handler(this::renderImageRegion);
        router.get(
                "/webclient/render_image/:imageId/:theZ/:theT*")
            .handler(this::renderImageRegion);

        // ShapeMask request handlers
        router.get(
                "/webgateway/render_shape_mask/:shapeId*")
            .handler(this::renderShapeMask);

        int port = config.getInteger("port");
        log.info("Starting HTTP server *:{}", port);
        server.requestHandler(router).listen(port,
            new Handler<AsyncResult<HttpServer>>() {
                @Override
                public void handle(AsyncResult<HttpServer> result) {
                    if (result.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(result.cause());
                    }
                }
            }
        );
    }

    /**
     * Exit point method which when the verticle stops, cleans up our current
     * OMERO.web session store.
     */
    @Override
    public void stop() throws Exception {
        sessionStore.close();
        tracing.close();
        if (spanReporter != null) {
            spanReporter.close();
        }
        if (sender != null) {
            sender.close();
        }
    }

    /**
     * Get information about microservice.
     * Confirms that this is a microservice
     * @param event Current routing context.
     */
    private void getMicroserviceDetails(RoutingContext event) {
        log.info("Getting Microservice Details");
        String version = Optional.ofNullable(
            this.getClass().getPackage().getImplementationVersion()
        ).orElse("development");
        int maxTileLength = (Integer) context.getBean("maxTileLength");
        JsonObject resData = new JsonObject()
                .put("provider", "ImageRegionMicroservice")
                .put("version", version)
                .put("features", new JsonArray()
                                 .add("flip")
                                 .add("mask-color")
                                 .add("png-tiles"))
                .put("options",new JsonObject()
                               .put("maxTileLength", maxTileLength));
        if (!cacheControlHeader.equals("")) {
            resData.getJsonObject("options").put("cacheControl", cacheControlHeader);
         }
        event.response()
            .putHeader("content-type", "application/json")
            .end(resData.encodePrettily());
    }

    /**
     * Render image region event handler.
     * Responds with an image body on success based on the <code>imageId</code>,
     * <code>z</code> and <code>t</code> encoded in the URL or HTTP 404 if the
     * {@link Image} does not exist or the user does not have permissions to
     * access it.
     * @param event Current routing context.
     */
    private void renderImageRegion(RoutingContext event) {
        log.info("Rendering image region");
        HttpServerRequest request = event.request();
        final ImageRegionCtx imageRegionCtx;
        try {
            imageRegionCtx = new ImageRegionCtx(
                    request.params(), event.get("omero.session_key"));
        } catch (IllegalArgumentException e) {
            HttpServerResponse response = event.response();
            response.setStatusCode(400).end(e.getMessage());
            return;
        }
        imageRegionCtx.injectCurrentTraceContext();

        final HttpServerResponse response = event.response();
        vertx.eventBus().<byte[]>request(
            ImageRegionVerticle.RENDER_IMAGE_REGION_EVENT,
            Json.encode(imageRegionCtx), new Handler<AsyncResult<Message<byte[]>>>() {
                @Override
                public void handle(AsyncResult<Message<byte[]>> result) {
                    try {
                        if (result.failed()) {
                            Throwable t = result.cause();
                            int statusCode = 404;
                            if (t instanceof ReplyException) {
                                statusCode = ((ReplyException) t).failureCode();
                            }
                            if (!response.closed()) {
                                response.setStatusCode(statusCode).end();
                            }
                            return;
                        }
                        byte[] imageRegion = result.result().body();
                        String contentType = "application/octet-stream";
                        if (imageRegionCtx.format.equals("jpeg")) {
                            contentType = "image/jpeg";
                        }
                        if (imageRegionCtx.format.equals("png")) {
                            contentType = "image/png";
                        }
                        if (imageRegionCtx.format.equals("tif")) {
                            contentType = "image/tiff";
                        }
                        response.headers().set("Content-Type", contentType);
                        response.headers().set(
                                "Content-Length",
                                String.valueOf(imageRegion.length));
                        if(!cacheControlHeader.equals("")) {
                            response.headers().set("Cache-Control", cacheControlHeader);
                        }
                        if (!response.closed()) {
                            response.end(Buffer.buffer(imageRegion));
                        }
                    } finally {
                        log.debug("Response ended");
                    }
                }
            }
        );
    }

    /**
     * Render shape mask event handler.
     * Responds with a <code>image/png</code> body on success based
     * on the <code>shapeId</code> encoded in the URL or HTTP 404 if the
     * {@link Shape} does not exist or the user does not have permissions to
     * access it.
     * @param event Current routing context.
     */
    private void renderShapeMask(RoutingContext event) {
        log.info("Rendering shape mask");
        HttpServerRequest request = event.request();
        ShapeMaskCtx shapeMaskCtx = new ShapeMaskCtx(
                request.params(), event.get("omero.session_key"));
        shapeMaskCtx.injectCurrentTraceContext();

        final HttpServerResponse response = event.response();
        vertx.eventBus().<byte[]>request(
            ShapeMaskVerticle.RENDER_SHAPE_MASK_EVENT,
            Json.encode(shapeMaskCtx), new Handler<AsyncResult<Message<byte[]>>>() {
                @Override
                public void handle(AsyncResult<Message<byte[]>> result){
                    try {
                        if (result.failed()) {
                            Throwable t = result.cause();
                            int statusCode = 404;
                            if (t instanceof ReplyException) {
                                statusCode = ((ReplyException) t).failureCode();
                            }
                            if (!response.closed()) {
                                response.setStatusCode(statusCode).end();
                            }
                            return;
                        }
                        byte[] shapeMask = result.result().body();
                        response.headers().set("Content-Type", "image/png");
                        response.headers().set(
                                "Content-Length",
                                String.valueOf(shapeMask.length));
                        if (!response.closed()) {
                            response.end(Buffer.buffer(shapeMask));
                        }
                    } finally {
                        log.debug("Response ended");
                    }
                }
            }
        );
    }

    /**
     * Retrieves the Hazelcast configuration either from the OMERO
     * configuration directory or Hazelcast defaults.
     */
    private Config getHazelcastConfig() {
        File configFile =
                new File(new File(new File("."), "etc"), "hazelcast.xml");
        Config config = new Config();
        if (configFile.exists()) {
            log.info("Loading Hazelcast configuration: {}",
                     configFile.getAbsolutePath());
            try (InputStream is = new FileInputStream(configFile);
                 InputStream bis = new BufferedInputStream(is)) {
                config = new XmlConfigBuilder(bis).build();
            } catch (IOException e) {
                log.error("Failed to read Hazelcast configuration", e);
            }
        } else {
            log.debug("Hazelcast configuration file {} not found",
                      configFile.getAbsolutePath());
        }
        return config;
    }
}
