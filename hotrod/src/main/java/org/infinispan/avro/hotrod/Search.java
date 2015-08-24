package org.infinispan.avro.hotrod;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;

/**
 * This class is the entry point to query Apache Avro defined types with Infinispan.
 * It return a query factory that is similar to the one employed for DSL queries
 * (see @see org.infinispan.client.hotrod.Search).
 *
 * Every field of an Avro defined type with a (String converted) length smaller
 * than 1000 bytes can be queried through the DSL.
 *
 * As with original HotRod queries, a query is executed locally at the server that receives it.
 *
 * @author Pierre Sutra
 */
public class Search {

   private Search() {}

   public static QueryFactory getQueryFactory(RemoteCache cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter cannot be null");
      }
      return new QueryFactory((RemoteCacheImpl) cache);
   }

}
