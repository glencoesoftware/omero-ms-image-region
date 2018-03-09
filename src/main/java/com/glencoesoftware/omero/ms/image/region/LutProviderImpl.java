package com.glencoesoftware.omero.ms.image.region;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ome.model.display.ChannelBinding;
import ome.services.scripts.RepoFile;
import ome.services.scripts.ScriptFileType;
import omeis.providers.re.lut.LutProvider;
import omeis.providers.re.lut.LutReader;
import omeis.providers.re.lut.LutReaderFactory;


/**
 * Lookup table provider implementation.
 * @author Chris Allan <callan@glencoesoftware.com>
 */
public class LutProviderImpl implements LutProvider {

    /** The logger for this class. */
    private static Logger log =
            LoggerFactory.getLogger(LutProviderImpl.class);

    /**
     * Available readers, keyed off name.  Should be an unmodifiable map.
     */
    protected final Map<String, LutReader> lutReaders =
            new HashMap<String, LutReader>();

    @SuppressWarnings("unchecked")
    public LutProviderImpl(File root, ScriptFileType lutType) {
        Iterator<File> scripts = FileUtils.iterateFiles(
                root, lutType.getFileFilter(), TrueFileFilter.TRUE);
        while (scripts.hasNext()) {
            RepoFile script = new RepoFile(root, scripts.next());
            String basename = script.basename();
            try {
                lutReaders.put(
                        basename, LutReaderFactory.read(script.file()));
                log.debug("Successfully added LUT '{}'", basename);
            } catch (Exception e) {
                log.warn("Cannot read lookup table: '{}'",
                        script.fullname(), e);
            }
        }
        log.info("Successfully added {} LUTs", lutReaders.size());
    }

    /* (non-Javadoc)
     * @see omeis.providers.re.lut.LutProvider#getLutReaders(ome.model.display.ChannelBinding[])
     */
    public List<LutReader> getLutReaders(ChannelBinding[] channelBindings) {
        log.debug("Looking up LUT readers for {} channels from {} LUTs",
                channelBindings.length, lutReaders.size());
        List<LutReader> toReturn = new ArrayList<LutReader>();
        for (ChannelBinding cb : channelBindings) {
            if (cb.getActive()) {
                toReturn.add(lutReaders.get(cb.getLookupTable()));
            }
        }
        return toReturn;
    }

}
