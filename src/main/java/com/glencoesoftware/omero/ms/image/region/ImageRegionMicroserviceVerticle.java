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
import java.util.Optional;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.glencoesoftware.omero.ms.core.OmeroVerticleFactory;
import com.glencoesoftware.omero.ms.core.OmeroWebJDBCSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebRedisSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionStore;
import com.glencoesoftware.omero.ms.core.PrometheusSpanHandler;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionRequestHandler;
import com.glencoesoftware.omero.ms.core.LogSpanReporter;
import com.glencoesoftware.omero.ms.core.OmeroHttpTracingHandler;

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
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.json.JsonCodec;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.micrometer.PrometheusScrapingHandler;
import ome.system.PreferenceContext;
import omero.model.Image;
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

    private static final String JMX_CONFIG =
        "---\n"
        + "startDelaySeconds: 0\n";

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionMicroserviceVerticle.class);

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /** OMERO server wide preference context. */
    private PreferenceContext preferences;

    /** OMERO.web session store */
    private OmeroWebSessionStore sessionStore;

    /** The string which will be used as Cache-Control header in responses */
    private String cacheControlHeader;

    /** VerticleFactory */
    private OmeroVerticleFactory verticleFactory;

    /** Default number of workers to be assigned to the worker verticle */
    private int DEFAULT_WORKER_POOL_SIZE;

    /** Default max number of channels to allow per request */
    private int MAX_ACTIVE_CHANNELS;

    /** Zipkin HTTP Tracing*/
    private HttpTracing httpTracing;

    private OkHttpSender sender;

    private AsyncReporter<Span> spanReporter;

    private Tracing tracing;

    static {
        com.glencoesoftware.omero.ms.core.SSLUtils.fixDisabledAlgorithms();
    }

    /**
     * Entry point method which starts the server event loop and initializes
     * our current OMERO.web session store.
     */
    @Override
    public void start(Promise<Void> prom) {
        log.info("Starting verticle");

        DEFAULT_WORKER_POOL_SIZE =
                Runtime.getRuntime().availableProcessors() * 2;

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
        retriever.getConfig(ar -> {
            try {
                deploy(ar.result(), prom);
            } catch (Exception e) {
                prom.fail(e);
            }
        });
    }

    /**
     * Deploys our verticles and performs general setup that depends on
     * configuration.
     * @param config Current configuration
     */
    public void deploy(JsonObject config, Promise<Void> prom) {
        log.info("Deploying verticle");

        // Set OMERO.server configuration options using system properties
        JsonObject omeroServer = config.getJsonObject("omero.server");
        if (omeroServer == null) {
            throw new IllegalArgumentException(
                    "'omero.server' block missing from configuration");
        }
        omeroServer.forEach(entry -> {
            System.setProperty(entry.getKey(), (String) entry.getValue());
        });

        context = new ClassPathXmlApplicationContext(
                "classpath:ome/config.xml",
                "classpath:ome/services/datalayer.xml",
                "classpath*:beanRefContext.xml",
                "classpath*:service-ms.core.PixelsService.xml");
        preferences =
                (PreferenceContext) this.context.getBean("preferenceContext");
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


        JsonObject vertxMetricsConfig =
                config.getJsonObject("vertx-metrics", new JsonObject());
        Boolean vertxMetricsEnabled =
                vertxMetricsConfig.getBoolean("enabled", false);
        if (vertxMetricsEnabled) {
            router.route("/vertxmetrics")
                .order(-3)
                .handler(PrometheusScrapingHandler.create());
            log.info("Vertx Metrics Enabled");
        } else {
            log.info("Vertx Metrics NOT Enabled");
        }

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

        // ImageData request handlers
        router.get("/webgateway/imgData/:imageId/:keys*").handler(this::getImageData);
        router.get("/webgateway/imgData/:imageId*").handler(this::getImageData);
        router.get("/pathviewer/imgData/:imageId/:keys*").handler(this::getImageData);
        router.get("/pathviewer/imgData/:imageId*").handler(this::getImageData);

        //histogram_json/(?P<iid>[0-9]+)/channel/(?P<theC>[0-9]+)/
        // Histogram request handlers
        router.get("/webgateway/histogram_json/:imageId/channel/:theC*")
            .handler(this::getHistogramJson);
        router.get("/pathviewer/histogram_json/:imageId/channel/:theC*")
            .handler(this::getHistogramJson);

        // ShapeMask request handlers
        router.get(
                "/webgateway/render_shape_mask/:shapeId*")
            .handler(this::renderShapeMask);
        router.get(
                "/omero_ms_image_region/get_shape_mask_bytes/:shapeId*")
            .handler(this::getShapeMaskBytes);
        router.get(
                "/omero_ms_image_region/get_label_image_metadata/:shapeId*")
            .handler(this::getLabelImageMetadata);

        // Thumbnail request handlers
        router.get(
                "/webclient/render_thumbnail_ngff/size/:longestSide/:imageId*")
            .handler(this::renderThumbnail);
        router.get(
                "/webclient/render_thumbnail_ngff/:imageId*")
            .handler(this::renderThumbnail);
        router.get(
                "/webgateway/render_thumbnail_ngff/:imageId/:longestSide*")
            .handler(this::renderThumbnail);
        router.get(
                "/webgateway/render_thumbnail_ngff/:imageId*")
            .handler(this::renderThumbnail);
        router.get(
                "/webclient/render_birds_eye_view_ngff/:imageId/:longestSide*")
            .handler(this::renderThumbnail);
        router.get(
                "/webclient/render_birds_eye_view_ngff/:imageId*")
            .handler(this::renderThumbnail);
        router.get(
                "/webgateway/render_birds_eye_view_ngff/:imageId/:longestSide*")
            .handler(this::renderThumbnail);
        router.get(
                "/webgateway/render_birds_eye_view_ngff/:imageId*")
            .handler(this::renderThumbnail);
        router.get(
                "/webgateway/get_thumbnails_ngff/:longestSide*")
            .handler(this::getThumbnails);
        router.get(
                "/webgateway/get_thumbnails_ngff*")
            .handler(this::getThumbnails);
        router.get(
                "/webclient/get_thumbnails_ngff/:longestSide*")
            .handler(this::getThumbnails);
        router.get(
                "/webclient/get_thumbnails_ngff*")
            .handler(this::getThumbnails);

        MAX_ACTIVE_CHANNELS = config.getInteger("max-active-channels", 10);

        int port = config.getInteger("port");
        log.info("Starting HTTP server *:{}", port);
        server.requestHandler(router).listen(port, result -> {
            if (result.succeeded()) {
                prom.complete();
            } else {
                prom.fail(result.cause());
            }
        });
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
     * If the result of the eventbus message is a failure, handle it and
     * return a response to the client.
     * @param result eventbus result from worker verticle
     * @param response HTTP response
     * @return whether or not the <code>result</code> failed
     */
    private <T> Boolean handleResultFailed(
            AsyncResult<Message<T>> result, HttpServerResponse response) {
        Boolean resultFailed = result.failed();
        if (resultFailed) {
            Throwable t = result.cause();
            int statusCode = 404;
            if (t instanceof ReplyException) {
                statusCode = ((ReplyException) t).failureCode();
            }
            if (statusCode < 200 || statusCode > 599) {
                log.error(
                    "Unexpected failureCode {} resetting to 500",
                    statusCode, t);
                statusCode = 500;
            }
            response.setStatusCode(statusCode);
        }
        return resultFailed;
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
        int maxTileLength = Integer.parseInt(
                Optional.ofNullable(
                    preferences.getProperty("omero.pixeldata.max_tile_length")
                ).orElse("2048").toLowerCase()
            );
        int maxPlaneWidth = Integer.parseInt(
                Optional.ofNullable(
                    preferences.getProperty("omero.pixeldata.max_plane_width")
                ).orElse("3192").toLowerCase()
            );
        int maxPlaneHeight = Integer.parseInt(
                Optional.ofNullable(
                    preferences.getProperty("omero.pixeldata.max_plane_height")
                ).orElse("3192").toLowerCase()
            );
        JsonObject resData = new JsonObject()
                .put("provider", "ImageRegionMicroservice")
                .put("version", version)
                .put("features", new JsonArray()
                                 .add("flip")
                                 .add("mask-color")
                                 .add("png-tiles")
                                 .add("quantization")
                                 .add("support-missing-channels"))
                .put("options",new JsonObject()
                               .put("maxTileLength", maxTileLength)
                               .put("maxPlaneWidth", maxPlaneWidth)
                               .put("maxPlaneHeight", maxPlaneHeight)
                               .put("maxActiveChannels", MAX_ACTIVE_CHANNELS));
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
            if (!response.closed()) {
                response.setStatusCode(400).end(e.getMessage());
            }
            return;
        }
        int activeChannelCount = 0;
        for (Integer channel : imageRegionCtx.channels) {
            if (channel > 0) activeChannelCount++;
        }
        if (activeChannelCount > MAX_ACTIVE_CHANNELS) {
            HttpServerResponse response = event.response();
            if (!response.closed()) {
                response.setStatusCode(400).end(String.format(
                    "Too many active channels. Cannot process more than " +
                    "%d per request", MAX_ACTIVE_CHANNELS));
            }
            return;
        }
        imageRegionCtx.injectCurrentTraceContext();

        final HttpServerResponse response = event.response();
        vertx.eventBus().<byte[]>request(
                ImageRegionVerticle.RENDER_IMAGE_REGION_EVENT,
                Json.encode(imageRegionCtx), result -> {
            try {
                if (handleResultFailed(result, response)) {
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
                if (!cacheControlHeader.equals("")) {
                    response.headers().set("Cache-Control", cacheControlHeader);
                }
                response.write(Buffer.buffer(imageRegion));
            } finally {
                if (!response.closed()) {
                    response.end();
                }
                log.debug("Response ended");
            }
        });
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
                Json.encode(shapeMaskCtx), result -> {
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                byte[] shapeMask = result.result().body();
                response.headers().set("Content-Type", "image/png");
                response.headers().set(
                        "Content-Length",
                        String.valueOf(shapeMask.length));
                response.write(Buffer.buffer(shapeMask));
            } finally {
                if (!response.closed()) {
                    response.end();
                }
                log.debug("Response ended");
            }
        });
    }

    /**
     * Get shape mask bytes event handler.
     * Responds with raws image bytes on success based
     * on the <code>shapeId</code> encoded in the URL or HTTP 404 if the
     * {@link Shape} does not exist or the user does not have permissions to
     * access it.
     * @param event Current routing context.
     */
    private void getShapeMaskBytes(RoutingContext event) {
        log.info("Getting shape mask bytes");
        HttpServerRequest request = event.request();
        ShapeMaskCtx shapeMaskCtx = new ShapeMaskCtx(
                request.params(), event.get("omero.session_key"));
        shapeMaskCtx.injectCurrentTraceContext();

        final HttpServerResponse response = event.response();
        vertx.eventBus().<byte[]>request(
                ShapeMaskVerticle.GET_SHAPE_MASK_BYTES_EVENT,
                Json.encode(shapeMaskCtx), result -> {
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                byte[] shapeMask = result.result().body();
                response.headers()
                        .set("Content-Type", "application/octet-stream");
                response.headers().set(
                        "Content-Length",
                        String.valueOf(shapeMask.length));
                response.write(Buffer.buffer(shapeMask));
            } finally {
                if (!response.closed()) {
                    response.end();
                }
                log.debug("Response ended");
            }
        });
    }

    /**
     * Get label image metadata event handler.
     * Responds with JSON payload of label image metadata on success based
     * on the <code>shapeId</code> encoded in the URL or HTTP 404 if the
     * LabelImage does not exist or the user does not have permissions to
     * access it.
     * @param event Current routing context.
     */
    private void getLabelImageMetadata(RoutingContext event) {
        log.info("Getting label image metadata");
        HttpServerRequest request = event.request();
        ShapeMaskCtx shapeMaskCtx = new ShapeMaskCtx(
                request.params(), event.get("omero.session_key"));
        shapeMaskCtx.injectCurrentTraceContext();

        final HttpServerResponse response = event.response();
        vertx.eventBus().<JsonObject>request(
                ShapeMaskVerticle.GET_LABEL_IMAGE_METADATA_EVENT,
                Json.encode(shapeMaskCtx), result -> {
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                JsonObject metadataJson = result.result().body();
                response.headers().set("Content-Type", "application/json");
                response.headers().set(
                        "Content-Length",
                        String.valueOf(metadataJson.encodePrettily().length()));
                response.write(metadataJson.encodePrettily());
            } finally {
                if (!response.closed()) {
                    response.end();
                }
                log.debug("Response ended");
            }
        });
    }

    /**
     * Get image data event handler.
     * Responds with JSON payload of image data on success based
     * on the <code>imageId</code> encoded in the URL or HTTP 404 if the
     * Image does not exist or the user does not have permissions to
     * access it.
     * @param event Current routing context.
     */
    private void getImageData(RoutingContext event) {
        log.info("Getting image data");
        HttpServerRequest request = event.request();
        final HttpServerResponse response = event.response();
        ImageDataCtx imageDataCtx = null;
        try {
            imageDataCtx = new ImageDataCtx(request.params(),
                event.get("omero.session_key"));
        } catch (Exception e) {
            log.error("Error creating ImageDataCtx", e);
            if (!response.closed()) {
                response.setStatusCode(400).end();
            }
            return;
        }
        imageDataCtx.injectCurrentTraceContext();
        vertx.eventBus().<JsonObject>request(
                ImageRegionVerticle.GET_IMAGE_DATA,
                Json.encode(imageDataCtx), result -> {
            String chunk = "";
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                JsonObject imgDataJson = result.result().body();
                Object toReturn = imgDataJson;
                if (request.params().contains("keys")) {
                    String[] keys = request.params().get("keys").split("\\.");
                    for (int i = 0; i < keys.length - 1; i++) {
                        imgDataJson = imgDataJson.getJsonObject(keys[i]);
                        if (imgDataJson == null) {
                            break;
                        }
                    }
                    if (imgDataJson == null) {
                        toReturn = null;
                    } else {
                        toReturn = imgDataJson.getValue(keys[keys.length - 1]);
                    }
                }
                chunk = JsonCodec.INSTANCE.toString(toReturn, true);
                if (request.params().contains("callback")) {
                    String callback = request.params().get("callback");
                    chunk = String.format("%s(%s)", callback, chunk);
                    response.headers().set("Content-Type",
                            "application/javascript");
                } else {
                    response.headers().set("Content-Type",
                            "application/json");
                }
            } finally {
                if (!response.closed()) {
                    response.end(chunk);
                }
            }
        });
    }


    /******* HISTOGRAM HANDLER **********/

    /**
     * Get histogram event handler.
     * @param event Current routing context.
     */
    private void getHistogramJson(RoutingContext event) {
        log.info("Getting histogram");
        int maxPlaneWidth = Integer.parseInt(
                Optional.ofNullable(
                    preferences.getProperty("omero.pixeldata.max_plane_width")
                ).orElse("3192").toLowerCase()
            );
        int maxPlaneHeight = Integer.parseInt(
                Optional.ofNullable(
                    preferences.getProperty("omero.pixeldata.max_plane_height")
                ).orElse("3192").toLowerCase()
            );
        HttpServerRequest request = event.request();
        HistogramCtx histogramCtx = null;
        request.params().add("maxPlaneWidth", Integer.toString(maxPlaneWidth));
        request.params().add("maxPlaneHeight", Integer.toString(maxPlaneHeight));
        try {
            histogramCtx = new HistogramCtx(request.params(),
                event.get("omero.session_key"));
        } catch (IllegalArgumentException e) {
            final HttpServerResponse response = event.response();
            if (!response.closed()) {
                response.setStatusCode(400).end(e.getMessage());
            }
            return;
        } catch (Exception e) {
            log.error("Error creating HistogramCtx", e);
            final HttpServerResponse response = event.response();
            if (!response.closed()) {
                response.setStatusCode(400).end();
            }
            return;
        }
        histogramCtx.injectCurrentTraceContext();
        vertx.eventBus().<JsonObject>request(
                ImageRegionVerticle.GET_HISTOGRAM_JSON,
                Json.encode(histogramCtx), result -> {
            final HttpServerResponse response = event.response();
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                JsonObject histogramData = result.result().body();
                response.headers().set("Content-Type", "application/json");
                response.headers().set(
                        "Content-Length",
                        String.valueOf(histogramData.encodePrettily().length()));
                response.write(histogramData.encodePrettily());
            } finally {
                if (!response.closed()) {
                    response.end();
                }
            }
        });
    }

    /******* THUMBNAIL HANDLERS *********/
    /**
     * Render thumbnail event handler for NGFF images. Responds with a
     * <code>image/jpeg</code> body on success based on the
     * <code>longestSide</code> and <code>imageId</code> encoded in the URL or
     * HTTP 404 if the {@link Image} does not exist or the user does not have
     * permissions to access it.
     * @param event Current routing context.
     */
    private void renderThumbnail(RoutingContext event) {
        final HttpServerRequest request = event.request();
        final HttpServerResponse response = event.response();
        final ThumbnailCtx thumbnailCtx;
        try {
            thumbnailCtx = new ThumbnailCtx(request.params(),
                    event.get("omero.session_key"));
        } catch (IllegalArgumentException e) {
            if (!response.closed()) {
                response.setStatusCode(400).end(e.getMessage());
            }
            return;
        }

        thumbnailCtx.injectCurrentTraceContext();
        vertx.eventBus().<byte[]>request(
                ImageRegionVerticle.RENDER_THUMBNAIL_EVENT,
                Json.encode(thumbnailCtx), result -> {
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                byte[] thumbnail = result.result().body();
                response.headers().set("Content-Type", "image/jpeg");
                response.headers().set(
                        "Content-Length",
                        String.valueOf(thumbnail.length));
                response.write(Buffer.buffer(thumbnail));
            } finally {
                if (!response.closed()) {
                    response.end();
                }
                log.debug("Response ended");
            }
        });
    }

    /**
     * Get thumbnails event handler for NGFF images. Responds with a JSON
     * dictionary of Base64 encoded <code>image/jpeg</code> thumbnails keyed
     * by {@link Image} identifier. Each dictionary value is prefixed with
     * <code>data:image/jpeg;base64,</code> so that it can be used with
     * <a href="http://caniuse.com/#feat=datauri">data URIs</a>.
     * @param event Current routing context.
     */
    private void getThumbnails(RoutingContext event) {
        final HttpServerRequest request = event.request();
        final HttpServerResponse response = event.response();
        final String callback = request.getParam("callback");
        final ThumbnailCtx thumbnailCtx;
        try {
            thumbnailCtx = new ThumbnailCtx(request.params(),
                    event.get("omero.session_key"));
        } catch (IllegalArgumentException e) {
            if (!response.closed()) {
                response.setStatusCode(400).end(e.getMessage());
            }
            return;
        }
        thumbnailCtx.injectCurrentTraceContext();

        vertx.eventBus().<String>request(
                ImageRegionVerticle.GET_THUMBNAILS_EVENT,
                Json.encode(thumbnailCtx), result -> {
            try {
                if (handleResultFailed(result, response)) {
                    return;
                }
                String json = result.result().body();
                String contentType = "application/json";
                if (callback != null) {
                    json = String.format("%s(%s);", callback, json);
                    contentType = "application/javascript";
                }
                response.headers().set("Content-Type", contentType);
                response.headers().set(
                        "Content-Length", String.valueOf(json.length()));
                response.write(json);
            } finally {
                if (!response.closed()) {
                    response.end();
                }
                log.debug("Response ended");
            }
        });
    }
}
