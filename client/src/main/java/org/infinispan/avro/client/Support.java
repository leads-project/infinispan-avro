package org.infinispan.avro.client;

import org.apache.avro.Schema;
import org.infinispan.commons.api.BasicCacheContainer;

/**
 * @author Pierre Sutra
 */
public class Support {

   public static final String AVRO_METADATA_CACHE_NAME = "__avro_metadata";
   public static final String DELIMITER = ".";
   public static final String DELIMITER_REGEX = "\\.";
   public static final String NULL = "__NULL__";

   public static void registerSchema(BasicCacheContainer container, Schema schema) {
      container.getCache(AVRO_METADATA_CACHE_NAME).put(schema.getFullName(), schema);
   }

}
