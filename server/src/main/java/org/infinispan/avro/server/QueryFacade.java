package org.infinispan.avro.server;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.lucene.search.Query;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.avro.client.Request;
import org.infinispan.avro.client.Response;
import org.infinispan.avro.client.SpecificMarshaller;
import org.infinispan.commons.logging.Log;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pierre Sutra
 */
public class QueryFacade implements org.infinispan.server.core.QueryFacade {

   private static final Log log = LogFactory.getLog(QueryFacade.class, Log.class);

   private SpecificMarshaller<Request> requestAvroMarshaller= new SpecificMarshaller<>(Request.class);
   private SpecificMarshaller<Response> responseAvroMarshaller = new SpecificMarshaller<>(Response.class);

   private Externalizer genericAvroMarshaller = new Externalizer();

   @Override
   public byte[] query(AdvancedCache<byte[], byte[]> cache, byte[] query) {

      Response response = new Response();

      try {

         Request request = (Request) requestAvroMarshaller.objectFromByteBuffer(query);
         log.debug(request.toString());
         final Schema schema = MetadataManager.getInstance().retrieveSchema(request.getSchemaName());

         SearchManager sm = Search.getSearchManager(cache);
         
         SearchIntegrator searchFactory = sm.getSearchFactory();
         QueryParser qp = new QueryParser();

         EntityNamesResolver resolver = s -> {
            if (s.equals(GenericData.Record.class.getName()))
               return GenericData.Record.class;
            return null;
         };

         LuceneProcessingChain processingChain
               = new LuceneProcessingChain.Builder(searchFactory,resolver).buildProcessingChainForDynamicEntities(
               (entityName, fieldName) -> ValueWrapperFieldBridge.retrieveFieldBridge(fieldName, schema));

         LuceneQueryParsingResult parsingResult = qp.parseQuery(request.getJpqlString().toString(), processingChain);
         Query q = parsingResult.getQuery();

         log.trace("Executing query "+q+ "["+q.getClass()+"]");

         // FIXME clustered queries are not properly working
         CacheQuery cacheQuery =
               request.getLocal()
                     ? sm.getQuery(q,GenericData.Record.class) : sm.getClusteredQuery(q,GenericData.Record.class);
         
         if (request.getMaxResult() > 0)
            cacheQuery = cacheQuery.maxResults(request.getMaxResult());
         if (parsingResult.getSort() != null)
            cacheQuery = cacheQuery.sort(parsingResult.getSort());
         if (request.getStartOffset() > 0)
            cacheQuery = cacheQuery.firstResult(request.getStartOffset().intValue());

         List<Object> list = cacheQuery.list();
         log.debug("#results="+list.size());
            
         List<ByteBuffer> results = new ArrayList<>();
         if (parsingResult.getProjections().size()==0){
            for (Object o: list) {
               assert o!=null;
               results.add(ByteBuffer.wrap((byte[])o));
            }
         }else{
            for (Object o: list) {
               GenericData.Record record = (GenericData.Record) genericAvroMarshaller.objectFromByteBuffer((byte[]) o);
               GenericRecordBuilder builder = new GenericRecordBuilder(record.getSchema());
               GenericData.Record copy = builder.build();
               for(Schema.Field f : record.getSchema().getFields()){
                  if (parsingResult.getProjections().contains(f.name()))
                     copy.put(f.name(),record.get(f.name()));
               }
               results.add(ByteBuffer.wrap(genericAvroMarshaller.objectToByteBuffer(copy)));
            }
         }
      
         response.setNumResults(cacheQuery.getResultSize());
         response.setResults(results);

      } catch (Exception e) {
         e.printStackTrace();
         response.setNumResults(0);
         response.setResults(new ArrayList<ByteBuffer>());
      }

      try {
         return responseAvroMarshaller.objectToByteBuffer(response);
      } catch (IOException e) {
         e.printStackTrace();
      }
      
      return null;

   }

}
