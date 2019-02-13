package com.glencoesoftware.omero.ms.image.region;

import ome.model.core.Pixels;
import ome.model.display.RenderingDef;
import ome.io.nio.PixelBuffer;

public class RendererInfo {
    public Pixels pixels;
    public PixelBuffer PixelBuffer;
    public RenderingDef renderingDef;

    public RendererInfo(Pixels pixels, PixelBuffer pixelBuffer, RenderingDef renderingDef) {
        this.pixels = pixels;
        this.PixelBuffer = pixelBuffer;
        this.renderingDef = renderingDef;
    }
}
