package com.glencoesoftware.omero.ms.image.region;

import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

public class OmeroVertxLauncher extends Launcher {
    @Override
    public void beforeStartingVertx(VertxOptions options) {
        options.setMetricsOptions(
                new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setEnabled(true));
    }

    public static void main(String[] args) {
        new OmeroVertxLauncher().dispatch(args);
    }
}
