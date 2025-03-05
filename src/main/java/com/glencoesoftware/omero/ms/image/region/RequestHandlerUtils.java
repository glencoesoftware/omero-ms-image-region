package com.glencoesoftware.omero.ms.image.region;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;

import brave.ScopedSpan;
import brave.Tracing;
import omero.RType;
import omero.ServerError;
import omero.sys.ParametersI;

public class RequestHandlerUtils {

    private static final org.slf4j.Logger log = LoggerFactory
            .getLogger(RequestHandlerUtils.class);

    /**
     * Whether or not a single {@link IObject} can be read from the server.
     * @param client OMERO client to use for querying.
     * @param className The name of the class to query for
     * @param id The id of the Object to query for
     * @return <code>true</code> if the {@link IObject} can be loaded or
     * <code>false</code> otherwise.
     * @throws ServerError If there was any sort of error retrieving the object.
     */
    public static boolean canRead(omero.client client, String className, long id) {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(id);
        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("can_read");
        try {
            String query = String.format("SELECT ann.id FROM %s as ann " +
                            "WHERE ann.id = :id", className);
            List<List<RType>> rows = client.getSession()
                    .getQueryService().projection(query, params, ctx);
            if (rows.size() > 0) {
                return true;
            }
        } catch (Exception e) {
            span.error(e);
            log.error("Exception while checking annotation readability", e);
        } finally {
            span.finish();
        }
        return false;
    }
}
