package com.glencoesoftware.omero.ms.image.region;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import ome.api.IScale;
import ome.api.local.LocalCompress;
import ome.model.enums.Family;
import ome.model.enums.RenderingModel;
import omeis.providers.re.lut.LutProvider;
import omero.RType;
import omero.api.IPixelsPrx;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;


public class ThumbnailRequestHandler extends ThumbnailsRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ThumbnailsRequestHandler.class);

    /**
     * {@link RenderingDef} identifier of the rendering settings to use when
     * requesting the thumbnail.
     */
    protected Optional<Long> renderingDefId;

    public ThumbnailRequestHandler(
            ThumbnailCtx thumbnailCtx,
            RenderingUtils renderingUtils,
            LocalCompress compressionSrv,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            IScale iScale,
            NgffUtils ngffUtils,
            String ngffDir,
            int longestSide,
            Long imageId) {
        super(thumbnailCtx, renderingUtils, compressionSrv, families, renderingModels, lutProvider, iScale, ngffUtils,
                ngffDir, longestSide, Arrays.asList(imageId));
    }

    public ThumbnailRequestHandler(
            ThumbnailCtx thumbnailCtx,
            RenderingUtils renderingUtils,
            LocalCompress compressionSrv,
            List<Family> families,
            List<RenderingModel> renderingModels,
            LutProvider lutProvider,
            IScale iScale,
            NgffUtils ngffUtils,
            String ngffDir,
            int longestSide,
            List<Long> imageIds,
            Optional<Long> renderingDefId) {
        super(thumbnailCtx, renderingUtils, compressionSrv, families, renderingModels, lutProvider, iScale, ngffUtils,
                ngffDir, longestSide, imageIds);
        this.renderingDefId = renderingDefId;
    }

    /**
     * Retrieves a JPEG thumbnail from the server.
     * @return JPEG thumbnail byte array.
     */
    public byte[] renderThumbnail(omero.client client) {
        log.info("renderThumbnail");
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("render_image_region");
        try {
            ServiceFactoryPrx sf = client.getSession();
            long userId = sf.getAdminService().getEventContext().userId;
            IQueryPrx iQuery = sf.getQueryService();
            IPixelsPrx iPixels = sf.getPixelsService();
            List<RType> pixelsIdAndSeries = RenderingUtils.getPixelsIdAndSeries(
                    iQuery, thumbnailCtx.imageId);
            if (pixelsIdAndSeries != null && pixelsIdAndSeries.size() == 2) {
                return getRegion(iQuery, iPixels, pixelsIdAndSeries, userId);
            }
            log.debug("Cannot find Image:{}", thumbnailCtx.imageId);
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while retrieving image region", e);
        } finally {
            span.finish();
        }
        return null;
    }

}
