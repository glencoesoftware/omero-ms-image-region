package com.glencoesoftware.omero.ms.image.region;

import java.awt.Dimension;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.imageio.ImageIO;

import io.vertx.core.MultiMap;
import loci.formats.ChannelFiller;
import loci.formats.ChannelSeparator;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import ome.conditions.ResourceError;
import ome.io.bioformats.BfPixelBuffer;
import ome.io.nio.PixelBuffer;
import ome.model.core.Channel;
import ome.model.core.Pixels;
import ome.model.display.ChannelBinding;
import ome.model.display.QuantumDef;
import ome.model.display.RenderingDef;
import ome.model.enums.Family;
import ome.model.enums.PixelsType;
import ome.model.enums.RenderingModel;
import ome.services.scripts.ScriptFileType;
import ome.util.ImageUtil;
import omeis.providers.re.Renderer;
import omeis.providers.re.data.PlaneDef;
import omeis.providers.re.data.RegionDef;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.quantum.QuantizationException;
import omeis.providers.re.quantum.QuantumFactory;
import omero.ServerError;
import ucar.ma2.Array;
import ucar.ma2.DataType;

public class CliHelper {

    String filePath;

    public CliHelper() {
    }

    public void testRender(String filePath, String outputPath, String tileString) throws IOException {
        BfPixelBuffer pixelBuffer = createBfPixelBuffer(filePath, 0);
        RenderingDef renderingDef = getRenderingDef();
        try {
            BufferedImage image = getBufferedImage(render(pixelBuffer, renderingDef, tileString));
            ImageIO.write(image, "jpg", new File(outputPath));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        finally {
            pixelBuffer.close();
        }

    }

    public void testRender(String filePath, String outputPath) throws IOException {
        String tileString = "0,0,0,1024,1024";
        testRender(filePath, outputPath, tileString);
    }

    public void testRender(String filePath) throws IOException {
        String outputPath = "/home/kevin/Pictures/clitest.jpg";
        String tileString = "0,0,0,1024,1024";
        testRender(filePath, outputPath, tileString);
    }

    private BfPixelBuffer createBfPixelBuffer(final String filePath,
            final int series) {
        try
        {
            IFormatReader reader = createBfReader();
            BfPixelBuffer pixelBuffer = new BfPixelBuffer(filePath, reader);
            pixelBuffer.setSeries(series);
            System.out.println(String.format("Creating BfPixelBuffer: %s Series: %d",
                    filePath, series));
            return pixelBuffer;
        } catch (Exception e)
        {
            String msg = "Error instantiating pixel buffer: " + filePath;
            e.printStackTrace();
            throw new ResourceError(msg);
        }
    }

    private RenderingDef getRenderingDef() {
        RenderingDef rdef = new RenderingDef();
        QuantumDef qdef = new QuantumDef();
        qdef.setBitResolution(QuantumFactory.DEPTH_8BIT);
        qdef.setCdStart(0);
        qdef.setCdEnd(255);
        rdef.setQuantization(qdef);
        //rdef.setModel(new RenderingModel(RenderingModel.VALUE_RGB));
        rdef.setModel(new RenderingModel("rgb"));
        ChannelBinding cb1 = new ChannelBinding();
        cb1.setFamily(new Family(Family.VALUE_LINEAR));
        cb1.setCoefficient(1.0);
        cb1.setNoiseReduction(false);
        cb1.setInputStart(0.0);
        cb1.setInputEnd(255.0);
        cb1.setActive(true);
        cb1.setRed(255);
        cb1.setGreen(0);
        cb1.setBlue(0);
        cb1.setAlpha(255);
        ChannelBinding cb2 = new ChannelBinding();
        cb2.setFamily(new Family(Family.VALUE_LINEAR));
        cb2.setCoefficient(1.0);
        cb2.setNoiseReduction(false);
        cb2.setInputStart(0.0);
        cb2.setInputEnd(255.0);
        cb2.setActive(true);
        cb2.setRed(0);
        cb2.setGreen(255);
        cb2.setBlue(0);
        cb2.setAlpha(255);
        ChannelBinding cb3 = new ChannelBinding();
        cb3.setFamily(new Family(Family.VALUE_LINEAR));
        cb3.setCoefficient(1.0);
        cb3.setNoiseReduction(false);
        cb3.setInputStart(0.0);
        cb3.setInputEnd(255.0);
        cb3.setActive(true);
        cb3.setRed(0);
        cb3.setGreen(0);
        cb3.setBlue(255);
        cb3.setAlpha(255);
        rdef.addChannelBinding(cb1);
        rdef.addChannelBinding(cb2);
        rdef.addChannelBinding(cb3);
        return rdef;
    }

    /**
     * Create an {@link IFormatReader} with the appropriate {@link loci.formats.ReaderWrapper}
     * instances and {@link IFormatReader#setFlattenedResolutions(boolean)} set to false.
     */
    private IFormatReader createBfReader() {
        IFormatReader reader = new ImageReader();
        reader = new ChannelFiller(reader);
        reader = new ChannelSeparator(reader);
        reader.setFlattenedResolutions(false);
        reader.setMetadataFiltered(true);
        return reader;
    }

    final private long imageId = 123;
    final private int z = 0;
    final private int t = 0;
    final private float q = 0.8f;
    // tile
    /*
    final private int resolution = 0;
    final private int tileX = 2;
    final private int tileY = 1;
    final private String tile = String.format(
            "%d,%d,%d,2048,2048", resolution, tileX, tileY);
            */
    // region
    final private int regionX = 0;
    final private int regionY = 0;
    final private int regionWidth = 1024;
    final private int regionHeight = 1024;
    final private String region = String.format(
            "%d,%d,%d,%d", regionX, regionY, regionWidth, regionHeight);
    // Channel info
    final private int channel0 = 1;
    final private int channel1 = 2;
    final private int channel2 = 3;
    final private double[] window0 = new double[]{0, 255};
    final private double[] window1 = new double[]{0, 255};
    final private double[] window2 = new double[]{0, 255};
    final private String color0 = "0000FF";
    final private String color1 = "00FF00";
    final private String color2 = "FF0000";
    final private String c = String.format(
            "%d|%f:%f$%s,%d|%f:%f$%s,%d|%f:%f$%s",
            channel0, window0[0], window0[1], color0,
            channel1, window1[0], window1[1], color1,
            channel2, window2[0], window2[1], color2);
    final private String maps = "[{\"reverse\": {\"enabled\": false}}, " +
            "{\"reverse\": {\"enabled\": false}}, " +
            "{\"reverse\": {\"enabled\": false}}]";

    private MultiMap params;

    private ImageRegionCtx getImageRegionCtx(String tileString) {
        params = MultiMap.caseInsensitiveMultiMap();
        params.add("imageId", String.valueOf(imageId));
        params.add("theZ", String.valueOf(z));
        params.add("theT", String.valueOf(t));
        //params.add("q", String.valueOf(q));

        params.add("tile", tileString);
        params.add("c", c);

        params.add("region", region);
        params.add("c", c);
        params.add("maps", maps);
        params.add("m", "c");

        return new ImageRegionCtx(params, "");
    }

    private LutProvider getLutProvider() {
        ScriptFileType stype = new ScriptFileType("*.lut", "text/x-lut");
        return new LutProviderImpl(new File("/home/kevin/omero/OMERO.server/lib/scripts/"), stype);
    }


    /**
     * Performs conditional rendering.
     * @param pixels pixels metadata
     * @param renderingDef rendering settings to use
     * @return Image region as packed integer array of shape [Y, X] ready for
     * compression.
     * @throws ServerError
     * @throws IOException
     * @throws QuantizationException
     */
    private Array render(PixelBuffer pixelBuffer, RenderingDef renderingDef,
            String tileString)
                    throws ServerError, IOException, QuantizationException {
        List<Family> families = new ArrayList<Family>();
        families.add(new Family(Family.VALUE_LINEAR));
        List<RenderingModel> renderingModels = new ArrayList<RenderingModel>();
        renderingModels.add(new RenderingModel(RenderingModel.VALUE_RGB));
        QuantumFactory quantumFactory = new QuantumFactory(families);
        Renderer renderer = null;
        Pixels pixels = getPixels(pixelBuffer);
        renderer = new Renderer(
            quantumFactory, renderingModels, pixels, renderingDef,
            pixelBuffer, getLutProvider());
        ImageRegionCtx imageRegionCtx = getImageRegionCtx(tileString);
        int t = Optional.ofNullable(imageRegionCtx.t)
                .orElse(renderingDef.getDefaultT());
        int z = Optional.ofNullable(imageRegionCtx.z)
                .orElse(renderingDef.getDefaultZ());
        PlaneDef planeDef = new PlaneDef(PlaneDef.XY, t);
        planeDef.setZ(z);
        // Avoid asking for resolution descriptions if there is no image
        // pyramid.  This can be *very* expensive.
        imageRegionCtx.setResolutionLevel(renderer, pixelBuffer);
        Integer sizeX = pixels.getSizeX();
        Integer sizeY = pixels.getSizeY();
        RegionDef regionDef = getRegionDef(sizeX, sizeY, pixelBuffer, imageRegionCtx);
        planeDef.setRegion(regionDef);
        checkPlaneDef(sizeX, sizeY, planeDef);

        /*
        if (imageRegionCtx.compressionQuality != null) {
            compressionSrv.setCompressionLevel(
                    imageRegionCtx.compressionQuality);
        }
        */
        imageRegionCtx.updateSettings(renderer, families, renderingModels);
        PixelBuffer newBuffer = null;
        /*
        if (imageRegionCtx.projection != null) {
            newBuffer = prepareProjectedPixelBuffer(pixels, renderer);
            planeDef = new PlaneDef(PlaneDef.XY, 0);
            planeDef.setZ(0);
        }
        */
        // RegionDef is updated by the rendering operation to the reflect
        // the size of the resulting array and consequently must happen
        // first.
        int[] buf = renderer.renderAsPackedInt(planeDef, newBuffer);
        return Array.factory(
                DataType.INT,
                new int[] { regionDef.getHeight(), regionDef.getWidth() },
                buf);
    }

    private BufferedImage getBufferedImage(Array array) throws IOException {
        Integer sizeY = array.getShape()[0];
        Integer sizeX = array.getShape()[1];
        int[] buf = (int[]) array.getStorage();
        /*
        buf = flip(buf, sizeX, sizeY,
                imageRegionCtx.flipHorizontal, imageRegionCtx.flipVertical);
                */
        return ImageUtil.createBufferedImage(
            buf, sizeX, sizeY
        );
    }

    private Pixels getPixels(PixelBuffer pixelBuffer) {
        Pixels pixels = new Pixels(null, null, pixelBuffer.getSizeX(),
                pixelBuffer.getSizeY(), pixelBuffer.getSizeZ(),
                pixelBuffer.getSizeC(), pixelBuffer.getSizeZ(), "", null);
        Channel c1 = new Channel();
        c1.setRed(255);
        c1.setGreen(0);
        c1.setBlue(0);
        c1.setAlpha(255);
        Channel c2 = new Channel();
        c2.setRed(0);
        c2.setGreen(255);
        c2.setBlue(0);
        c2.setAlpha(255);
        Channel c3 = new Channel();
        c3.setRed(0);
        c3.setGreen(0);
        c3.setBlue(255);
        c3.setAlpha(255);
        pixels.addChannel(c1);
        pixels.addChannel(c2);
        pixels.addChannel(c3);
        pixels.setSizeC(3);
        PixelsType type = new PixelsType(PixelsType.VALUE_UINT8);
        type.setBitSize(8);
        pixels.setPixelsType(type);
        pixels.setSignificantBits(8);

        return pixels;
    }

    private void truncateRegionDef(
            int sizeX, int sizeY, RegionDef regionDef) {
        if (regionDef.getX() > sizeX ||
                regionDef.getY() > sizeY) {
            throw new IllegalArgumentException(String.format(
                    "Start position (%d,%d) exceeds image size (%d, %d)",
                    regionDef.getX(), regionDef.getY(), sizeX, sizeY));
        }
        regionDef.setWidth(Math.min(
                regionDef.getWidth(), sizeX - regionDef.getX()));
        regionDef.setHeight(Math.min(
                regionDef.getHeight(), sizeY - regionDef.getY()));
    }

    /**
     * Update RegionDef to be flipped if required.
     * @param sizeX width of the image at the current resolution
     * @param sizeY height of the image at the current resolution
     * @param tileSize XY tile sizes of the underlying pixels
     * @param regionDef region definition to flip if required
     * @throws IllegalArgumentException
     * @throws ServerError
     * @see ImageRegionRequestHandler#getRegionDef(Pixels, PixelBuffer)
     */
    private void flipRegionDef(int sizeX, int sizeY, RegionDef regionDef,
            ImageRegionCtx imageRegionCtx) {
        if (imageRegionCtx.flipHorizontal) {
            regionDef.setX(
                    sizeX - regionDef.getWidth() - regionDef.getX());
        }
        if (imageRegionCtx.flipVertical) {
            regionDef.setY(
                    sizeY - regionDef.getHeight() - regionDef.getY());
        }
    }

    /**
     * Returns RegionDef to read based on tile / region provided in
     * ImageRegionCtx.
     * @param resolutionLevels complete definition of all resolution levels
     * @param pixelBuffer raw pixel data access buffer
     * @return RegionDef {@link RegionDef} describing image region to read
     * @throws IllegalArgumentException
     * @throws ServerError
     */
    private RegionDef getRegionDef(
            Integer sizeX, Integer sizeY, PixelBuffer pixelBuffer,
            ImageRegionCtx imageRegionCtx)
                    throws IllegalArgumentException, ServerError {
        RegionDef regionDef = new RegionDef();
        Dimension imageTileSize = pixelBuffer.getTileSize();
        if (imageRegionCtx.tile != null) {
            int tileSizeX = imageRegionCtx.tile.getWidth();
            int tileSizeY = imageRegionCtx.tile.getHeight();
            if (tileSizeX == 0) {
                tileSizeX = (int) imageTileSize.getWidth();
            }
            if (tileSizeY == 0) {
                tileSizeY = (int) imageTileSize.getHeight();
            }
            regionDef.setWidth(tileSizeX);
            regionDef.setHeight(tileSizeY);
            regionDef.setX(imageRegionCtx.tile.getX() * tileSizeX);
            regionDef.setY(imageRegionCtx.tile.getY() * tileSizeY);
        } else if (imageRegionCtx.region != null) {
            regionDef.setX(imageRegionCtx.region.getX());
            regionDef.setY(imageRegionCtx.region.getY());
            regionDef.setWidth(imageRegionCtx.region.getWidth());
            regionDef.setHeight(imageRegionCtx.region.getHeight());
        } else {
            regionDef.setX(0);
            regionDef.setY(0);
            regionDef.setWidth(sizeX);
            regionDef.setHeight(sizeY);
            return regionDef;
        }
        truncateRegionDef(sizeX, sizeY, regionDef);
        flipRegionDef(sizeX, sizeY, regionDef, imageRegionCtx);
        return regionDef;
    }

    /**
     * Flip an image horizontally, vertically, or both.
     * @param src source image buffer
     * @param sizeX size of <code>src</code> in X (number of columns)
     * @param sizeY size of <code>src</code> in Y (number of rows)
     * @param flipHorizontal whether or not to flip the image horizontally
     * @param flipVertical whether or not to flip the image vertically
     * @return Newly allocated buffer with flipping applied or <code>src</code>
     * if no flipping has been requested.
     */
    private int[] flip(
            int[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        if (!flipHorizontal && !flipVertical) {
            return src;
        }

        if (src == null) {
            throw new IllegalArgumentException("Attempted to flip null image");
        } else if (sizeX == 0 || sizeY == 0) {
            throw new IllegalArgumentException(
                    "Attempted to flip image with 0 size");
        }

        int[] dest = new int[src.length];
        int srcIndex, destIndex;
        int xOffset = flipHorizontal? sizeX : 1;
        int yOffset = flipVertical? sizeY : 1;
        for (int x = 0; x < sizeX; x++) {
            for (int y = 0; y < sizeY; y++) {
                srcIndex = (y * sizeX) + x;
                destIndex = Math.abs(((yOffset - y - 1) * sizeX))
                        + Math.abs((xOffset - x - 1));
                dest[destIndex] = src[srcIndex];
            }
        }
        return dest;
    }

    private void checkPlaneDef(
            Integer sizeX, Integer sizeY, PlaneDef planeDef){
        RegionDef rd = planeDef.getRegion();
        if (rd == null) {
            return;
        }
        if (rd.getWidth() + rd.getX() > sizeX) {
            int newWidth = sizeX - rd.getX();
            rd.setWidth(newWidth);
        }
        if (rd.getHeight() + rd.getY() > sizeY) {
            int newHeight = sizeY - rd.getY();
            rd.setHeight(newHeight);
        }
    }
}
