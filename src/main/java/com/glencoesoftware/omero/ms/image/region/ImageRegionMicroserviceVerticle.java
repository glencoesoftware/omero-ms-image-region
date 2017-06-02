package com.glencoesoftware.omero.ms.image.region;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Cookie;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CookieHandler;
import omero.model.Image;

public class ImageRegionMicroserviceVerticle extends AbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageRegionMicroserviceVerticle.class);

    /**
     * Entry point method which starts the server event loop and initializes
     * our current OMERO.web session store.
     */
    @Override
    public void start(Future<Void> future) {
        log.info("Starting verticle");

        if (config().getBoolean("debug")) {
            Logger root = (Logger) LoggerFactory.getLogger(
                    "com.glencoesoftware.omero.ms");
            root.setLevel(Level.DEBUG);
        }

        // Deploy our dependency verticles
        JsonObject omero = config().getJsonObject("omero");
        vertx.deployVerticle(new ImageRegionVerticle(
                omero.getString("host"), omero.getInteger("port")),
                new DeploymentOptions().setWorker(true).setMultiThreaded(true));

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        // Cookie handler so we can pick up the OMERO.web session
        router.route().handler(CookieHandler.create());

        // OMERO session handler which picks up the session key from the
        // OMERO.web session and joins it.
        JsonObject redis = config().getJsonObject("redis");

        router.route().handler(event -> {
            Cookie cookie = event.getCookie("sessionid");
            if (cookie == null) {
                event.response().setStatusCode(403);
                event.response().end();
            }
            String sessionKey = cookie.getValue();
            log.debug("OMERO.web session key: {}", sessionKey);
            event.next();
        });

        log.info("Heyho");
        // ImageRegion request handlers
        router.get(
                "/webgateway/render_image_region/:imageId/:z/:t*")
            .handler(this::renderImageRegion);

        int port = config().getInteger("port");
        log.info("Starting HTTP server *:{}", port);
        server.requestHandler(router::accept).listen(port, result -> {
            if (result.succeeded()) {
                future.complete();
            } else {
                future.fail(result.cause());
            }
        });
    }

    /**
     * Exit point method which when the verticle stops, cleans up our current
     * OMERO.web session store.
     */
    @Override
    public void stop() throws Exception {
        
    }

    /**
     * Render thumbnail event handler. Responds with a <code>image/jpeg</code>
     * body on success based on the <code>longestSide</code> and
     * <code>imageId</code> encoded in the URL or HTTP 404 if the {@link Image}
     * does not exist or the user does not have permissions to access it.
     * @param event Current routing context.
     */
    private void renderImageRegion(RoutingContext event) {
        log.info("Rendering image region");
        HttpServerRequest request = event.request();
        ImageRegionCtx imageRegionCtx = new ImageRegionCtx(request);
        Map<String, Object> data = imageRegionCtx.getImageRegionFormatted();
        data.put("omeroSessionKey", event.get("omero.session_key"));
        final HttpServerResponse response = event.response();
        log.info("Received request with data: {}", data);
        response.headers().set("Content-Type", "json");
        String message = data.toString();
        response.headers().set(
                "Content-Length",
                String.valueOf(message.length()));
        response.write(Buffer.buffer(message));
        response.end();
    }

}
