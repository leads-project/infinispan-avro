package org.infinispan.avro.hotrod;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.commons.CacheException;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author Pierre Sutra
 */
public class QueryFactory extends BaseQueryFactory<Query> {

   private RemoteCacheImpl cache;

   public QueryFactory(RemoteCacheImpl c){
      this.cache = c;
   }

   @Override
   public org.infinispan.query.dsl.QueryBuilder from(Class entityType) {
      if (!GenericContainer.class.isAssignableFrom(entityType))
         throw new CacheException();
      try {
         GenericContainer container = (GenericContainer) entityType.newInstance();
         Schema schema = container.getSchema();
         return new QueryBuilder(cache,this,schema);
      } catch (InstantiationException | IllegalAccessException e) {
         e.printStackTrace();
      }
      throw new CacheException();
   }

   @Override
   public org.infinispan.query.dsl.QueryBuilder from(String entityType) {
      try {
         return from(Class.forName(entityType));
      } catch (ClassNotFoundException e) {
         e.printStackTrace();
      }
      throw new CacheException();
   }

}
