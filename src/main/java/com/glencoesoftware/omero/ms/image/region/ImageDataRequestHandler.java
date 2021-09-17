package com.glencoesoftware.omero.ms.image.region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.json.JsonObject;
import ome.model.containers.Dataset;
import ome.model.containers.Project;
import ome.model.core.Image;
import ome.model.core.Pixels;
import omero.ApiUsageException;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.ServiceFactoryPrx;
import omero.model.IObject;
import omero.model.WellSampleI;
import omero.sys.ParametersI;
import omero.util.IceMapper;

public class ImageDataRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ImageDataRequestHandler.class);

    private ImageDataCtx imageDataCtx;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects.
     */
    protected final IceMapper mapper = new IceMapper();

    public ImageDataRequestHandler(ImageDataCtx imageDataCtx) {
        this.imageDataCtx = imageDataCtx;
    }

    public JsonObject getImageData(omero.client client) {
        log.info("In ReqeustHandler::getImageData");
        try {
            Long imageId = imageDataCtx.imageId;
            ServiceFactoryPrx sf = client.getSession();
            IQueryPrx iQuery = sf.getQueryService();
            Image image = queryImageData(iQuery, imageId);
            log.info(image.toString());
            JsonObject imgData = new JsonObject();
            imgData.put("id", imageId);
            JsonObject meta = new JsonObject();
            meta.put("imageName", image.getName());
            meta.put("imageDescription", image.getDescription());
            //meta.put("imageAuthor", image.getAuthor())
            List<Dataset> datasets = image.linkedDatasetList();
            if(datasets.size() > 1) {
                meta.put("datasetName", "Multiple");
                Set<Long> projectIds = new HashSet<Long>();
                for(Dataset ds : datasets) {
                    List<Project> projects = ds.linkedProjectList();
                    if (projects.size() > 1) {
                        meta.put("projectName", "Multiple");
                        break;
                    } else {
                        if (projectIds.contains(projects.get(0).getId())) {
                            meta.put("projectName", "Multiple");
                            break;
                        } else {
                            projectIds.add(projects.get(0).getId());
                        }
                    }
                }
                if (!meta.containsKey("projectName")) {
                    Project project = datasets.get(0).linkedProjectList().get(0);
                    meta.put("projectName", project.getName());
                    meta.put("projectId", project.getId());
                    meta.put("projectDescription", project.getDescription());
                }
            } else if(datasets.size() == 1) {
                Dataset ds = datasets.get(0);
                meta.put("datasetName", ds.getName());
                meta.put("datasetId", ds.getId());
                meta.put("datasetDescription", ds.getDescription());
            }
            Optional<WellSampleI> wellSample = getWellSample(iQuery, imageId);
            if (wellSample.isPresent()) {
                meta.put("wellSampleId", wellSample.get().getId().getValue());
                meta.put("wellId", wellSample.get().getWell().getId().getValue());
            }
            meta.put("imageId", image.getId());
            meta.put("pixelsType", image.getPixels(0).getPixelsType().toString());
            imgData.put("meta", meta);

            JsonObject perms = new JsonObject();
            imgData.put("perms", perms);
            /*
                    "perms": {
                            "canAnnotate": image.canAnnotate(),
                            "canEdit": image.canEdit(),
                            "canDelete": image.canDelete(),
                            "canLink": image.canLink(),
                        },
                    }
            */

            return imgData;
        } catch (ServerError e) {
            log.error("Error getting image data");
        }
        return null;
    }

    protected Image queryImageData(
            IQueryPrx iQuery, Long imageId)
                throws ApiUsageException, ServerError {
        ScopedSpan span = Tracing.currentTracer()
                .startScopedSpan("retrieve_pix_description");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            span.tag("omero.image_id", imageId.toString());
            // Query pulled from ome.logic.PixelsImpl and expanded to include
            // our required Image / Plate metadata; loading both sides of the
            // Image <--> WellSample <--> Well collection so that we can
            // resolve our field index.
            ParametersI params = new ParametersI();
            params.addId(imageId);
            Image image = (Image) mapper.reverse(
                    iQuery.findByQuery(
                            "select i from Image as i " +
                            " join fetch i.pixels as pixels" +
                            " left outer JOIN FETCH i.datasetLinks as links " +
                            " left outer join fetch links.parent as dataset " +
                            " left outer join fetch dataset.projectLinks as plinks " +
                            " left outer join fetch plinks.parent as project " +
                            " where i.id=:id", params, ctx));
            return image;
        } finally {
            span.finish();
        }
    }

    public static Optional<WellSampleI> getWellSample(IQueryPrx iQuery, Long imageId) {
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("get_wellsample");
        try {
            Map<String, String> ctx = new HashMap<String, String>();
            ctx.put("omero.group", "-1");
            ParametersI params = new ParametersI();
            List<Long> ids = new ArrayList<Long>();
            ids.add(imageId);
            params.addIds(ids);
            List<IObject> wellSamples = iQuery.findAllByQuery(
                    "SELECT ws FROM WellSample AS ws" +
                    "  LEFT OUTER JOIN FETCH ws.well AS w" +
                    "  WHERE ws.image.id IN :ids",
                    params, ctx
                );
            for (IObject ob : wellSamples) {
                WellSampleI ws = (WellSampleI) ob;
                return Optional.of(ws);
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while retrieving image region", e);
        } finally {
            span.finish();
        }
        return Optional.empty();
    }
}
