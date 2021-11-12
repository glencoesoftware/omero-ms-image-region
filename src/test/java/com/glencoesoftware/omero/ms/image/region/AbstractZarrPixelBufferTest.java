/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.bc.zarr.ZarrGroup;
import com.glencoesoftware.bioformats2raw.Converter;

import picocli.CommandLine;

public abstract class AbstractZarrPixelBufferTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * Run the bioformats2raw main method and check for success or failure.
     *
     * @param additionalArgs CLI arguments as needed beyond "input output"
     */
    void assertBioFormats2Raw(Path input, Path output, String...additionalArgs)
            throws IOException {
        List<String> args = new ArrayList<String>(
                Arrays.asList(new String[] { "--compression", "null" }));
        for (String arg : additionalArgs) {
            args.add(arg);
        }
        args.add(input.toString());
        args.add(output.toString());
        try {
            Converter converter = new Converter();
            CommandLine cli = new CommandLine(converter);
            cli.execute(args.toArray(new String[]{}));
            Assert.assertTrue(Files.exists(output.resolve(".zattrs")));
            Assert.assertTrue(Files.exists(
                    output.resolve("OME").resolve("METADATA.ome.xml")));
        } catch (RuntimeException rt) {
            throw rt;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    static Path fake(String...args) {
        Assert.assertTrue(args.length %2 == 0);
        Map<String, String> options = new HashMap<String, String>();
        for (int i = 0; i < args.length; i += 2) {
            options.put(args[i], args[i+1]);
        }
        return fake(options);
    }

    static Path fake(Map<String, String> options) {
        return fake(options, null);
    }

    /**
     * Create a Bio-Formats fake INI file to use for testing.
     * @param options map of the options to assign as part of the fake filename
     * from the allowed keys
     * @param series map of the integer series index and options map (same format
     * as <code>options</code> to add to the fake INI content
     * @see https://docs.openmicroscopy.org/bio-formats/6.4.0/developers/
     * generating-test-images.html#key-value-pairs
     * @return path to the fake INI file that has been created
     */
    static Path fake(Map<String, String> options,
            Map<Integer, Map<String, String>> series)
    {
        return fake(options, series, null);
    }

    static Path fake(Map<String, String> options,
            Map<Integer, Map<String, String>> series,
            Map<String, String> originalMetadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("image");
        if (options != null) {
            for (Map.Entry<String, String> kv : options.entrySet()) {
                sb.append("&");
                sb.append(kv.getKey());
                sb.append("=");
                sb.append(kv.getValue());
            }
        }
        sb.append("&");
        try {
            List<String> lines = new ArrayList<String>();
            if (originalMetadata != null) {
                lines.add("[GlobalMetadata]");
                for (String key : originalMetadata.keySet()) {
                    lines.add(String.format(
                            "%s=%s", key, originalMetadata.get(key)));
                }
            }
            if (series != null) {
                for (int s : series.keySet()) {
                    Map<String, String> seriesOptions = series.get(s);
                    lines.add(String.format("[series_%d]", s));
                    for (String key : seriesOptions.keySet()) {
                        lines.add(String.format(
                                "%s=%s", key, seriesOptions.get(key)));
                    }
                }
            }
            Path ini = Files.createTempFile(sb.toString(), ".fake.ini");
            File iniAsFile = ini.toFile();
            String iniPath = iniAsFile.getAbsolutePath();
            String fakePath = iniPath.substring(0, iniPath.length() - 4);
            Path fake = Paths.get(fakePath);
            File fakeAsFile = fake.toFile();
            Files.write(fake, new byte[]{});
            Files.write(ini, lines);
            iniAsFile.deleteOnExit();
            fakeAsFile.deleteOnExit();
            return ini;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path writeTestZarr(
            int sizeT,
            int sizeC,
            int sizeZ,
            int sizeY,
            int sizeX,
            String pixelType,
            int resolutions) throws IOException {
        Path input = fake(
                "sizeT", Integer.toString(sizeT),
                "sizeC", Integer.toString(sizeC),
                "sizeZ", Integer.toString(sizeZ),
                "sizeY", Integer.toString(sizeY),
                "sizeX", Integer.toString(sizeX),
                "pixelType", pixelType,
                "resolutions", Integer.toString(resolutions));
        Path output = tmpDir.getRoot().toPath().resolve("output.zarr");
        assertBioFormats2Raw(input, output, "--pixel-type", pixelType);
        List<Object> msArray = new ArrayList<>();
        Map<String, Object> msData = new HashMap<>();
        Map<String, Object> msMetadata = new HashMap<>();
        msMetadata.put("method", "loci.common.image.SimpleImageScaler");
        msMetadata.put("version", "Bio-Formats 6.5.1");
        msData.put("metadata", msMetadata);
        msData.put("datasets", getDatasets(resolutions));
        msData.put("version", "0.1");
        msArray.add(msData);
        ZarrGroup z = ZarrGroup.open(output.resolve("0"));
        Map<String,Object> attrs = new HashMap<String, Object>();
        attrs.put("multiscales", msArray);
        z.writeAttributes(attrs);
        return output;
    }

    List<Map<String, String>> getDatasets(int resolutions) {
        List<Map<String, String>> datasets = new ArrayList<>();
        for (int i = 0; i < resolutions; i++) {
            Map<String, String> resObj = new HashMap<>();
            resObj.put("path", Integer.toString(i));
            datasets.add(resObj);
        }
        return datasets;
    }

}
