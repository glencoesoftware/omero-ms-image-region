package com.glencoesoftware.omero.ms.image.region;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import ome.model.core.Image;
import ome.model.core.Pixels;
import ome.model.enums.PixelsType;

public class MemoRegenerator {
    private static ConfigRetriever retriever;

    private static HashMap<Integer, String> pixelsTypes;

    private static String csvPath;

    public MemoRegenerator() {
        Vertx vertx = Vertx.vertx();
        ConfigStoreOptions store = new ConfigStoreOptions()
                .setType("file")
                .setFormat("yaml")
                .setConfig(new JsonObject()
                        .put("path", "conf/config.yaml")
                )
                .setOptional(true);
        retriever = ConfigRetriever.create(
                vertx, new ConfigRetrieverOptions()
                        .setIncludeDefaultStores(true)
                        .addStore(store));

        pixelsTypes = new HashMap<Integer, String>();
        pixelsTypes.put(1, "bit");
        pixelsTypes.put(2, "int8");
        pixelsTypes.put(5, "uint8");
        pixelsTypes.put(3, "int16");
        pixelsTypes.put(6, "uint16");
        pixelsTypes.put(4, "int32");
        pixelsTypes.put(7, "uint32");
        pixelsTypes.put(8, "float");
        pixelsTypes.put(9, "double");
        pixelsTypes.put(10, "complex");
        pixelsTypes.put(11, "double-complex");
    }

    public static void main(String[] args) {
        System.out.println("Running MemoRegenerator main");
        if(args.length < 1) {
            System.err.println("Must provide a csv file path");
            return;
        }
        System.out.println(args[0]);
        csvPath = args[0];
        MemoRegenerator mr = new MemoRegenerator();
        retriever.getConfig(mr::withConfig);
    }

    private void withConfig(AsyncResult<JsonObject> result) {
        JsonObject config = result.result();
        // Set OMERO.server configuration options using system properties
        JsonObject omeroServer = config.getJsonObject("omero.server");
        if (omeroServer == null) {
            throw new IllegalArgumentException(
                    "'omero.server' block missing from configuration");
        }
        omeroServer.forEach(entry -> {
            System.setProperty(entry.getKey(), (String) entry.getValue());
        });


        try (BufferedReader csvReader = new BufferedReader(new FileReader(csvPath))) {
            ApplicationContext context = new ClassPathXmlApplicationContext(
                    "classpath:ome/config.xml",
                    "classpath:ome/services/datalayer.xml",
                    "classpath*:beanRefContext.xml",
                    "classpath*:service-ms.core.PixelsService.xml");

            String line;
            while((line = csvReader.readLine()) != null) {
                try {
                    String[] imageParams = line.split(",");
                    Long imageId = Long.valueOf(imageParams[0]);
                    Long pixelsId = Long.valueOf(imageParams[1]);
                    Integer series = Integer.valueOf(imageParams[2]);
                    Integer ptInt = Integer.valueOf(imageParams[3]);
                    Integer sizeX = Integer.valueOf(imageParams[4]);
                    Integer sizeY = Integer.valueOf(imageParams[5]);

                    Image image = new Image(imageId, true);
                    image.setSeries(series);

                    Pixels pixels = new Pixels(pixelsId, true);
                    pixels.setImage(image);
                    PixelsType pt = new PixelsType(pixelsTypes.get(ptInt));
                    pixels.setPixelsType(pt);
                    pixels.setSizeX(sizeX);
                    pixels.setSizeY(sizeY);

                    PixelsService pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");

                    PixelBuffer pb = pixelsService.getPixelBuffer(pixels, false);
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                    System.err.println(e.toString());
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred");
            System.err.println(e.toString());
            System.err.println(e.getMessage());
            e.printStackTrace();
        } finally {
            Vertx.currentContext().owner().close();
        }
    }
}
