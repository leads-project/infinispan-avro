package org.infinispan.avro.hotrod;

import org.apache.avro.Schema;
import org.infinispan.avro.client.Response;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.query.dsl.Query;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Pierre Sutra
 */
public class RemoteQuery implements Query, Cloneable {

   private static final Log log = LogFactory.getLog(RemoteQuery.class);

   protected RemoteCacheImpl cache;
   protected String jpqlString;
   protected Schema schemaName;
   protected long startOffset; //FIXME can this really be long or it has to be int due to limitations in query module?
   protected InetSocketAddress location;
   protected List results;
   protected int numResults;
   protected int maxResults;

   public RemoteQuery(RemoteCacheImpl cache, String jpqlString, Schema schemaName, long startOffset, int maxResults) {
      this.cache = cache;
      this.jpqlString = jpqlString;
      this.schemaName = schemaName;
      this.startOffset = startOffset;
      this.maxResults = maxResults;
   }

   public RemoteCacheImpl getCache() {
      return cache;
   }

   public String getJpqlString() {
      return jpqlString;
   }

   public long getStartOffset() {
      return startOffset;
   }

   public int getMaxResults() {
      return maxResults;
   }

   public void setLocation(InetSocketAddress dest){
      this.location = dest;
   }
   
   public InetSocketAddress getLocation(){
      return this.location;
   }

   protected List<Object> executeQuery() {

      List<Object> results;
      QueryOperation op = ((OperationsFactory)cache.getOperationsFactory()).newAvroQueryOperation(this);
      Response response = op.execute();
      results = new ArrayList<>(response.getResults().size());
      for (ByteBuffer byteBuffer : response.getResults()) {
         try {
            results.add(cache.getRemoteCacheManager().getMarshaller().objectFromByteBuffer(byteBuffer.array()));
         } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
         }
      }
      numResults = response.getNumResults();
      return results;
   }

   @Override
   public Query setParameter(String s, Object o) {
      log.warn("Unsupported operation.");
      return this;
   }

   @Override
   public Query setParameters(Map<String, Object> map) {
      log.warn("Unsupported operation.");
      return this;
   }

   @Override
   @SuppressWarnings("unchecked")
   public synchronized <T> List<T> list() {
      if (results == null) {
         results = executeQuery();
      }

      return (List<T>) results;
   }

   @Override
   public int getResultSize() {
      list();
      return numResults;
   }
   
   @Override
   public String toString(){
      return jpqlString + "(schemaName = "+schemaName+", max="+maxResults+", offset="+startOffset+")";
   }
   
   @Override
   public Object clone() throws CloneNotSupportedException {
      RemoteQuery query = (RemoteQuery) super.clone();
      query.cache = this.cache;
      query.jpqlString = this.jpqlString;
      query.schemaName = this.schemaName;
      query.startOffset = this.startOffset;
      query.location = this.location;
      query.results = this.results;
      query.numResults = this.numResults;
      query.maxResults = this.maxResults;
      return query;
   }

}
