package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
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
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.glencoesoftware.bioformats2raw.Converter;

import brave.Tracing;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.util.PixelData;
import picocli.CommandLine;
import ucar.ma2.InvalidRangeException;
import zipkin2.reporter.Reporter;

public class ZarrPixelBufferTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * Run the bioformats2raw main method and check for success or failure.
     *
     * @param additionalArgs CLI arguments as needed beyond "input output"
     */
    void assertBioFormats2Raw(Path input, Path output, String...additionalArgs) throws IOException {
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
      }
      catch (RuntimeException rt) {
        throw rt;
      }
      catch (Throwable t) {
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
              lines.add(String.format("%s=%s", key, originalMetadata.get(key)));
            }
          }
          if (series != null) {
            for (int s : series.keySet()) {
              Map<String, String> seriesOptions = series.get(s);
              lines.add(String.format("[series_%d]", s));
              for (String key : seriesOptions.keySet()) {
                lines.add(String.format("%s=%s", key, seriesOptions.get(key)));
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
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      public Path writeTestZarr(String zarrName,
              String sizeT,
              String sizeC,
              String sizeZ,
              String sizeY,
              String sizeX,
              String pixelType,
              String resolutions) throws IOException {
          Path input = fake("sizeT", sizeT,
                  "sizeC", sizeC,
                  "sizeZ", sizeZ,
                  "sizeY", sizeY,
                  "sizeX", sizeX,
                  "pixelType", pixelType,
                  "resolutions", resolutions);
          Path output = tmpDir.newFolder().toPath().resolve(zarrName);
          assertBioFormats2Raw(input, output, "--pixel-type", pixelType);
          List<Object> msArray = new ArrayList<>();
          Map<String, Object> msData = new HashMap<>();
          Map<String, Object> msMetadata = new HashMap<>();
          msMetadata.put("method", "loci.common.image.SimpleImageScaler");
          msMetadata.put("version", "Bio-Formats 6.5.1");
          msData.put("metadata", msMetadata);
          msData.put("datasets", getDatasets(Integer.valueOf(resolutions)));
          msData.put("version", "0.1");
          msArray.add(msData);
          System.out.println(msArray.toString());
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

      @Test
      public void testGetChunks() throws IOException {
          Path output = writeTestZarr("testGetChunks.zarr",
                  "1",
                  "3",
                  "1",
                  "512",
                  "2048",
                  "uint16",
                  "3");
          try(ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 1024)) {
              int[][] chunks = zpbuf.getChunks();
              int[][] expectedChunks = new int[][] {
                      new int[] {1, 1, 1, 128, 512},
                      new int[] {1, 1, 1, 256, 1024},
                      new int[] {1, 1, 1, 512, 1024}};
              for(int i = 0; i < chunks.length; i++) {
                  Assert.assertTrue(Arrays.equals(chunks[i], expectedChunks[i]));
              }
          }
      }

      @Test
      public void testGetDatasets() throws IOException {
          Path output = writeTestZarr("test2.zarr",
                  "1",
                  "3",
                  "1",
                  "512",
                  "2048",
                  "uint16",
                  "3");
          try(ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 1024)){
              List<Map<String,String>> datasets = zpbuf.getDatasets();
              List<Map<String,String>> expectedDatasets = getDatasets(3);
              for(int i = 0; i < datasets.size(); i++) {
                  Assert.assertEquals(datasets.get(i), expectedDatasets.get(i));
              }
          }
      }

      @Test
      public void testGetShapes() throws IOException {
          Path output = writeTestZarr("test2.zarr",
                  "1",
                  "2",
                  "3",
                  "512",
                  "2048",
                  "uint16",
                  "3");
          try (ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 1024)){
              int[][] shapes = zpbuf.getShapes();
              int[][] expectedShapes = new int[][] {
                  new int[] {1, 2, 3, 128, 512},
                  new int[] {1, 2, 3, 256, 1024},
                  new int[] {1, 2, 3, 512, 2048}};
              for(int i = 0; i < shapes.length; i++) {
                  System.out.println(Arrays.toString(shapes[i]));
                  Assert.assertTrue(Arrays.equals(shapes[i], expectedShapes[i]));
              }
              List<List<Integer>> resolutionDescriptions = zpbuf.getResolutionDescriptions();
              for(int i = 0; i < resolutionDescriptions.size(); i++) {
                  List<Integer> desc = resolutionDescriptions.get(i);
                  Assert.assertEquals(desc.size(), 2);
                  Assert.assertEquals(desc.get(0), Integer.valueOf(expectedShapes[i][4]));
                  Assert.assertEquals(desc.get(1), Integer.valueOf(expectedShapes[i][3]));
              }
              zpbuf.setResolutionLevel(0);
              Assert.assertEquals(zpbuf.getSizeT(), 1);
              Assert.assertEquals(zpbuf.getSizeC(), 2);
              Assert.assertEquals(zpbuf.getSizeZ(), 3);
              Assert.assertEquals(zpbuf.getSizeY(), 128);
              Assert.assertEquals(zpbuf.getSizeX(), 512);
              zpbuf.setResolutionLevel(1);
              Assert.assertEquals(zpbuf.getSizeT(), 1);
              Assert.assertEquals(zpbuf.getSizeC(), 2);
              Assert.assertEquals(zpbuf.getSizeZ(), 3);
              Assert.assertEquals(zpbuf.getSizeY(), 256);
              Assert.assertEquals(zpbuf.getSizeX(), 1024);
              zpbuf.setResolutionLevel(2);
              Assert.assertEquals(zpbuf.getSizeT(), 1);
              Assert.assertEquals(zpbuf.getSizeC(), 2);
              Assert.assertEquals(zpbuf.getSizeZ(), 3);
              Assert.assertEquals(zpbuf.getSizeY(), 512);
              Assert.assertEquals(zpbuf.getSizeX(), 2048);
          }
      }

      @Test
      public void test() throws IOException {
          Path output = writeTestZarr("test2.zarr",
                  "1",
                  "2",
                  "3",
                  "512",
                  "2048",
                  "uint16",
                  "3");
          try (ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 1024)) {
              int[][] shapes = zpbuf.getShapes();
              int[][] expectedShapes = new int[][] {
                  new int[] {1, 2, 3, 128, 512},
                  new int[] {1, 2, 3, 256, 1024},
                  new int[] {1, 2, 3, 512, 2048}};
              for(int i = 0; i < shapes.length; i++) {
                  System.out.println(Arrays.toString(shapes[i]));
                  Assert.assertTrue(Arrays.equals(shapes[i], expectedShapes[i]));
              }
          }
      }

      @Test
      public void testGetTile() throws IOException, InvalidRangeException {
          Path output = writeTestZarr("pbtest.zarr",
                  "2",
                  "3",
                  "4",
                  "5",
                  "6",
                  "int32",
                  "1");
          ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
          int[] data = new int[2*3*4*5*6];
          for (int i = 0; i < 2*3*4*5*6; i++) {
              data[i] = i;
          }
          System.out.println(test.getDataType().toString());
          Tracing.newBuilder().spanReporter(Reporter.NOOP).build();
          test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
          try (ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 1024)) {
              PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 2, 2);
              ByteBuffer bb = pixelData.getData();
              bb.order(ByteOrder.BIG_ENDIAN);
              IntBuffer ib = bb.asIntBuffer();
              Assert.assertEquals(ib.get(0), 0);
              Assert.assertEquals(ib.get(1), 1);
              Assert.assertEquals(ib.get(2), 6);
              Assert.assertEquals(ib.get(3), 7);
              pixelData = zpbuf.getTile(1, 1, 1, 1, 1, 2, 2);
              bb = pixelData.getData();
              bb.order(ByteOrder.BIG_ENDIAN);
              ib = bb.asIntBuffer();
              Assert.assertEquals(ib.get(0), 517);//360(6*5*4*3) + 120(6*5*4) + 30(6*5) + 6 + 1
              Assert.assertEquals(ib.get(1), 518);
              Assert.assertEquals(ib.get(2), 523);
              Assert.assertEquals(ib.get(3), 524);
          }
      }

      @Test(expected = DimensionsOutOfBoundsException.class)
      public void testGetTileLargerThanImage() throws IOException, InvalidRangeException {
          Path output = writeTestZarr("pbtest.zarr",
                  "2",
                  "3",
                  "4",
                  "5",
                  "6",
                  "int32",
                  "1");
          ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
          int[] data = new int[2*3*4*5*6];
          for (int i = 0; i < 2*3*4*5*6; i++) {
              data[i] = i;
          }
          System.out.println(test.getDataType().toString());
          Tracing.newBuilder().spanReporter(Reporter.NOOP).build();
          test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
          try (ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 1024)) {
              zpbuf.setResolutionLevel(0);
              PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 10, 10);
              ByteBuffer bb = pixelData.getData();
              bb.order(ByteOrder.BIG_ENDIAN);
              IntBuffer ib = bb.asIntBuffer();
              Assert.assertEquals(ib.get(0), 0);
              Assert.assertEquals(ib.get(1), 1);
              Assert.assertEquals(ib.get(2), 6);
              Assert.assertEquals(ib.get(3), 7);
          }
      }

      @Test
      public void testTileExceedsMax() throws IOException, InvalidRangeException {
          Path output = writeTestZarr("tileSizeError.zarr",
                  "1",
                  "2",
                  "3",
                  "512",
                  "2048",
                  "uint16",
                  "3");
          ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
          int[] data = new int[2*3*4*5*6];
          for (int i = 0; i < 2*3*4*5*6; i++) {
              data[i] = i;
          }
          System.out.println(test.getDataType().toString());
          Tracing.newBuilder().spanReporter(Reporter.NOOP).build();
          test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
          try (ZarrPixelBuffer zpbuf = new ZarrPixelBuffer(output.resolve("0"), 32)) {
              PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 1, 33);
              Assert.assertNull(pixelData);
          }
      }


}
