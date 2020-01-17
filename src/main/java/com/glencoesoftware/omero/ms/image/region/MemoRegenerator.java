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

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.univocity.parsers.common.processor.ObjectRowListProcessor;
import com.univocity.parsers.conversions.Conversions;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelsService;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.enums.PixelsType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name="memoregenerator",
    description = "Regenerates Bio Formats memo files")
public class MemoRegenerator implements Callable<Void> {

    private static final Logger log =
            LoggerFactory.getLogger(MemoRegenerator.class);

    @Option(names = {"-h", "--help"},
            usageHelp = true,
            description = "display this help message")
    boolean usageHelpRequested;

    @Option(
        names = "--inplace",
        description = "set instead of --cache-dir to create the memo files " +
                "in place instead of copying them to a new directory. " +
                "This WILL modify the memo files from the pixels service " +
                "configured cache directory"
        )
    private boolean inplace;

    @Option(
        names = "--cache-dir",
        description =
            "specify additional directory for Bio-Formats cache.  Memo files " +
            "from the pixels service configured cache directory will be " +
            "copied to this directory if they exist and regenerated as " +
            "required using the Bio-Formats version of the microservice.  " +
            "No memo files from the pixels service configured cache " +
            "directory will be modified."
    )
    private Path cacheDir;

    @Parameters(
        index = "0",
        arity = "1",
        description =
            "path to CSV with Images to regenerate memo files for; " +
            "expected columns are: imageId, pixelsId, series, pixelsType, " +
            "sizeX, sizeY"
    )
    private Path csv;

    private ClassPathXmlApplicationContext context;

    private PixelsService pixelsService;

    @Override
    public Void call() throws Exception {
        final Vertx vertx = Vertx.vertx();
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
        CompletableFuture<JsonObject> future =
                new CompletableFuture<JsonObject>();
        retriever.getConfig(result -> {
            vertx.close();
            if (!result.failed()) {
                future.complete(result.result());
            } else {
                future.completeExceptionally(result.cause());
            }
        });
        JsonObject config = future.get();
        try {
            init(config);
            regen();
        } finally {
            if (context != null) {
                context.close();
            }
        }
        return null;
    }

    private void init(JsonObject config) {
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
        pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");
        if (cacheDir == null && inplace == false) {
            throw new IllegalArgumentException("Must supply a cache-dir with --cache-dir or use the --inplace option.");
        } else if (cacheDir != null && inplace == true) {
            throw new IllegalArgumentException("Cannot set both --inplace and --cache-dir");
        }
        if (cacheDir != null) {
            pixelsService.setMemoizerDirectoryLocal(cacheDir.toString());
        }
    }

    private void regen() {
        // imageId, pixelsId, series, pixelsType, sizeX, sizeY
        ObjectRowListProcessor rowProcessor = new ObjectRowListProcessor();
        rowProcessor.convertIndexes(Conversions.toLong()).set(0, 1);
        rowProcessor.convertIndexes(Conversions.toInteger()).set(2, 4, 5);

        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setProcessor(rowProcessor);
        parserSettings.setLineSeparatorDetectionEnabled(true);

        log.info("Loading Image data from CSV: {}", csv);
        CsvParser parser = new CsvParser(parserSettings);
        parser.parse(csv.toFile());

        int total = rowProcessor.getRows().size();
        int i = 1;
        for (Object[] row : rowProcessor.getRows()) {
            log.info(String.format("Processing row %d of %d", i, total));
            try {
                Pixels pixels = pixelsFromRow(row);
                pixelsService.getPixelBuffer(pixels, false);
            } catch(Exception e) {
                log.error(String.format("Caught exception processing row %d", i), e);
            } finally {
                i++;
            }
        }
    }

    private Pixels pixelsFromRow(Object[] row) {
        Long imageId = (Long) row[0];
        Long pixelsId = (Long) row[1];
        Integer series = (Integer) row[2];
        String pixelsType = (String) row[3];
        Integer sizeX = (Integer) row[4];
        Integer sizeY = (Integer) row[5];

        Image image = new Image(imageId, true);
        image.setSeries(series);

        Pixels pixels = new Pixels(pixelsId, true);
        pixels.setImage(image);
        PixelsType pt = new PixelsType(pixelsType);
        pixels.setPixelsType(pt);
        pixels.setSizeX(sizeX);
        pixels.setSizeY(sizeY);
        return pixels;
    }

    public static void main(String[] args) {
        CommandLine.call(new MemoRegenerator(), args);
    }

}
