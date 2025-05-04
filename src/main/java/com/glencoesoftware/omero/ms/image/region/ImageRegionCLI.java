package com.glencoesoftware.omero.ms.image.region;

public class ImageRegionCLI {

    public static void main(String[] args) {
        System.out.println("Hello, world!");
        if (args.length < 1) {
            System.out.println("Please supply a file name");
        }
        String filePath = args[0];
        CliHelper cliHelper = new CliHelper();
        try {
            if (args.length == 1) {
                cliHelper.testRender(filePath);
            } else if (args.length == 2) {
                String outputPath = args[1];
                cliHelper.testRender(filePath, outputPath);
            } else if (args.length == 3) {
                String outputPath = args[1];
                String tileString = args[2];
                cliHelper.testRender(filePath, outputPath, tileString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
