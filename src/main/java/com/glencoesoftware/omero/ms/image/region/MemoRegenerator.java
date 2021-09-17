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

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.yaml.snakeyaml.Yaml;

import com.univocity.parsers.common.processor.ObjectRowListProcessor;
import com.univocity.parsers.conversions.Conversions;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import loci.formats.FormatTools;
import ome.conditions.MissingPyramidException;
import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.enums.PixelsType;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    name = "memoregenerator", mixinStandardHelpOptions = true,
    description = "Regenerates Bio-Formats memo files",
    versionProvider = MemoRegenerator.ManifestVersionProvider.class
)
public class MemoRegenerator implements Callable<Void> {

    private static final Logger log =
            LoggerFactory.getLogger(MemoRegenerator.class);

    @ArgGroup(exclusive = true, multiplicity = "1")
    private Mode mode;

    static class Mode {
        @Option(
            names = "--inplace",
            required = true,
            description =
                "set instead of --cache-dir to create the memo files " +
                "in place instead of copying them to a new directory. " +
                "This WILL modify the memo files from the pixels service " +
                "configured cache directory"
        )
        private boolean inplace;

        @Option(
            names = "--cache-dir",
            required = true,
            description =
                "specify additional directory for Bio-Formats cache.  Memo " +
                "files from the pixels service configured cache directory " +
                "will be copied to this directory if they exist and " +
                "regenerated as required using the Bio-Formats version of " +
                "the microservice.  No memo files from the pixels service " +
                "configured cache directory will be modified."
        )
        private Path cacheDir;
    }

    static class ManifestVersionProvider implements IVersionProvider {

        @Override
        public String[] getVersion() throws Exception {
            String version = MemoRegenerator.class.getPackage()
                    .getImplementationVersion();
            if (version == null) {
                version = "DEV";
            }
            return new String[] {
                "omero-ms-image-region: " + version,
                "Bio-Formats Version: " + FormatTools.VERSION,
                "Bio-Formats Build date: " + FormatTools.DATE,
                "Bio-Formats VCS revision: " + FormatTools.VCS_REVISION
            };
        }

    }

    @Option(names = "--config", required = false, description = "Path to config.yml file")
    private String configPath = "conf/config.yaml";


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
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = new FileInputStream(configPath);
            Map<String, Object> config = yaml.load(inputStream);
            init(config);
            regen();
        } catch (Exception e) {
            log.error("Exception during memo regeneration.", e);
        }
        try {
        } finally {
            if (context != null) {
                context.close();
            }
        }
        return null;
    }

    private void init(Map<String, Object> config) {
        // Set OMERO.server configuration options using system properties
        Map<String, Object> omeroServer = (Map<String, Object>) config.get("omero.server");
        if (omeroServer == null) {
            throw new IllegalArgumentException(
                    "'omero.server' block missing from configuration");
        }
        omeroServer.forEach((key, value) -> {
            System.setProperty(key, (String) value);
        });

        context = new ClassPathXmlApplicationContext(
                "classpath:ome/config.xml",
                "classpath:ome/services/datalayer.xml",
                "classpath*:beanRefContext.xml",
                "classpath*:service-ms.core.PixelsService.xml");
        pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");
        if (mode.cacheDir != null) {
            pixelsService.setMemoizerDirectoryLocal(mode.cacheDir.toString());
        }
    }

    private void printSkip(int i, int total, long imageId, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        System.out.printf(
            "%d/%d - skip: %d %.3f%n",
            i, total, imageId, (float) elapsedTime/1000000);
    }

    private void regen() {
        // imageId, pixelsId, series, pixelsType, sizeX, sizeY
        ObjectRowListProcessor rowProcessor = new ObjectRowListProcessor();
        rowProcessor.convertIndexes(Conversions.toLong()).set(0, 1);
        rowProcessor.convertIndexes(Conversions.toInteger())
            .set(2, 4, 5, 6, 7, 8);

        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setProcessor(rowProcessor);
        parserSettings.setLineSeparatorDetectionEnabled(true);

        log.info("Loading Image data from CSV: {}", csv);
        CsvParser parser = new CsvParser(parserSettings);
        parser.parse(csv.toFile());

        int total = rowProcessor.getRows().size();
        int i = 1;
        int errorCount = 0;
        for (Object[] row : rowProcessor.getRows()) {
            log.info("Processing row {} of {}", i, total);
            long startTime = System.nanoTime();
            Long imageId = (Long) row[0];
            try {
                Pixels pixels = pixelsFromRow(row);

                Method getOriginalFilePath = pixelsService
                        .getClass()
                        .getSuperclass()
                        .getDeclaredMethod("getOriginalFilePath", Pixels.class);
                getOriginalFilePath.setAccessible(true);
                String originalFilePath = (String) getOriginalFilePath.invoke(
                        pixelsService, pixels);
                // Check whether or not an original file path can be looked up
                if (originalFilePath == null) {
                    log.warn(
                        "No original file path row {} ImageID {}", i, imageId
                    );
                    printSkip(i, total, imageId, startTime);
                    continue;
                }
                // We have an original file path in the managed repository,
                // check that the symlink exists
                Path p = Paths.get(originalFilePath);
                if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
                    log.warn(
                        "Managed repository symlink missing row {} ImageID {}",
                        i, imageId
                    );
                    printSkip(i, total, imageId, startTime);
                    continue;
                }
                // We have a symlink in the managed repository, assert that the
                // symlink is not dangling
                if (!Files.exists(p)) {
                    log.warn(
                        "Managed repository dangling symlink row {} ImageID {}",
                        i, imageId
                    );
                    printSkip(i, total, imageId, startTime);
                    continue;
                }

                PixelBuffer buffer =
                        pixelsService.getPixelBuffer(pixels, false);
                if (buffer != null) {
                    buffer.close();
                }
                long elapsedTime = System.nanoTime() - startTime;
                System.out.printf("%d/%d - ok: %d %.3f%n", i, total, imageId, (float) elapsedTime/1000000);
            } catch (MissingPyramidException e) {
                printSkip(i, total, imageId, startTime);
            } catch (Exception e) {
                log.error("Caught exception processing row {} ImageID {}", i, imageId, e);
                long elapsedTime = System.nanoTime() - startTime;
                System.out.printf("%d/%d - fail: %d %.3f%n", i, total, imageId, (float) elapsedTime/1000000);
                errorCount++;
            } finally {
                i++;
            }
        }
        log.info("COMPLETE: Processed {} images with {} failures", total, errorCount);
        System.out.printf("COMPLETE: processed %d rows with %d successes and %d failures%n",
                total, total - errorCount, errorCount);
    }

    private Pixels pixelsFromRow(Object[] row) {
        Long imageId = (Long) row[0];
        Long pixelsId = (Long) row[1];
        Integer series = (Integer) row[2];
        String pixelsType = (String) row[3];
        Integer sizeX = (Integer) row[4];
        Integer sizeY = (Integer) row[5];
        Integer sizeZ = (Integer) row[6];
        Integer sizeC = (Integer) row[7];
        Integer sizeT = (Integer) row[8];

        Image image = new Image(imageId, true);
        image.setSeries(series);

        Pixels pixels = new Pixels(pixelsId, true);
        pixels.setImage(image);
        PixelsType pt = new PixelsType(pixelsType);
        pixels.setPixelsType(pt);
        pixels.setSizeX(sizeX);
        pixels.setSizeY(sizeY);
        pixels.setSizeZ(sizeZ);
        pixels.setSizeC(sizeC);
        pixels.setSizeT(sizeT);
        return pixels;
    }

    public static void main(String[] args) {
        new CommandLine(new MemoRegenerator()).execute(args);
    }

}
