package com.glencoesoftware.omero.ms.image.region;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.esotericsoftware.minlog.Log;

import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.slf4j.LoggerFactory;

public class OmeroVertxLauncher extends Launcher {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(OmeroVertxLauncher.class);

    @Override
    public void beforeStartingVertx(VertxOptions options) {
        //Get config
        Boolean metricsEnabled = false;
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = new FileInputStream("conf/config.yaml");
            Map<String, Object> config = yaml.load(inputStream);
            metricsEnabled = (Boolean) ((Map<String, Object>) config.get("vertx-metrics")).get("enabled");
        } catch(Exception e) {
            log.error("Failed to load config properly.", e);
        }
        if (metricsEnabled) {
            options.setMetricsOptions(
                    new MicrometerMetricsOptions()
                    .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                    .setEnabled(true));
            log.info("Vertx Metrics Enabled");
        } else {
            log.info("Vertx Metrics Disabled");
        }
    }

    public static void main(String[] args) {
        new OmeroVertxLauncher().dispatch(args);
    }
}
