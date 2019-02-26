package com.glencoesoftware.omero.ms.image.region;

import java.util.List;
import java.util.Optional;

import ome.model.enums.Family;
import ome.model.enums.RenderingModel;

public class OmeroEnumerations {

    /** Available families */
    private Optional<List<Family>> families = Optional.empty();

    /** Available rendering models */
    private Optional<List<RenderingModel>> renderingModels = Optional.empty();

    public void setFamilies(List<Family> fams) {
        synchronized(families) {
            if(!families.isPresent()) {
                families = Optional.ofNullable(fams);
            }
        }
    }

    public List<Family> getFamilies(){
        synchronized(families) {
            if(families.isPresent()) {
                return families.get();
            } else {
                return null;
            }
        }
    }

    public void setRenderingModels(List<RenderingModel> rndModels) {
        synchronized(renderingModels) {
            if(!renderingModels.isPresent()) {
                renderingModels = Optional.ofNullable(rndModels);
            }
        }
    }

    public List<RenderingModel> getRenderingModels(){
        synchronized(renderingModels) {
            if(renderingModels.isPresent()) {
                return renderingModels.get();
            } else {
                return null;
            }
        }
    }
}
