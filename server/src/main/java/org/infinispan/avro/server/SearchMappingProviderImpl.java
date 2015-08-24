package org.infinispan.avro.server;

import org.apache.avro.generic.GenericData;
import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Norms;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;

/**
 * @author Pierre Sutra
 */
public class SearchMappingProviderImpl implements ProgrammaticSearchMappingProvider {
    @Override
    public void defineMappings(Cache cache, SearchMapping searchMapping) {
        searchMapping.entity(GenericData.Record.class)
                .indexed()
                .classBridgeInstance(new ValueWrapperFieldBridge())
                .norms(Norms.NO)
                .analyze(Analyze.NO)
                .store(Store.NO);
    }
}
