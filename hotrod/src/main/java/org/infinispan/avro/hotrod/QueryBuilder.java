package org.infinispan.avro.hotrod;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Pierre Sutra
 */
public class QueryBuilder extends BaseQueryBuilder<Query> {

   private static final Log log = LogFactory.getLog(QueryBuilder.class);

   private RemoteCacheImpl cache;
   private Schema schema;

   public QueryBuilder(RemoteCacheImpl cache, QueryFactory qf, Schema schema) {
      super(qf,GenericData.Record.class.getName());
      this.cache = cache;
      this.schema = schema;
   }

   @Override
   public Query build() {
      String jpqlString = accept(new JPAQueryGenerator());
      log.debug("JPQL string : "+jpqlString);
      return new RemoteQuery(cache, jpqlString, schema, startOffset, maxResults);
   }

   public Collection<RemoteQuery> split(Query query) {
      TransportFactory transportFactory
            = ((OperationsFactory)cache.getOperationsFactory()).getTransportFactory();
      if (!(query instanceof RemoteQuery))
         throw new CacheException("need an AvroRemoteQuery");
      if (!(transportFactory instanceof  TcpTransportFactory))
         throw new CacheException("need a TcpTransportFactory");
      Collection<SocketAddress> servers = ((TcpTransportFactory)transportFactory).getServers();
      Collection<RemoteQuery> results = new ArrayList<>();
      for (SocketAddress addr: servers) {
         InetSocketAddress address = (InetSocketAddress) addr;
         try {
            RemoteQuery q = (RemoteQuery) ((RemoteQuery) query).clone();
            q.setLocation(address);
            results.add(q);
         } catch (CloneNotSupportedException e) {
            e.printStackTrace();
            // not reachable.
         }
      }
      return results;
   }

}
