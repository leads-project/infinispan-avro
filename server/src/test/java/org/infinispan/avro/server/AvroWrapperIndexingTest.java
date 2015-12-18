package org.infinispan.avro.server;

import example.avro.DeviceList;
import example.avro.Employee;
import example.avro.User;
import example.avro.WebPage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.infinispan.avro.client.AbstractMarshaller;
import org.infinispan.avro.client.Support;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * @author Pierre Sutra
 */
@Test(groups = "functional", testName = "org.infinispan.avro.server.AvroWrapperIndexingTest")
public class AvroWrapperIndexingTest extends SingleCacheManagerTest {

   private static SimpleExternalizer externalizer = new SimpleExternalizer();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.indexing().enable()
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cfg.validate();
      
      Configuration configuration = cfg.build();
      assert cfg.clustering().cacheMode() == CacheMode.LOCAL;
      
      EmbeddedCacheManager cacheManager = new DefaultCacheManager(configuration);
      cacheManager.getCache();

      return cacheManager;
   }

   @Test
   public void testIndexingWithWrapper() throws Exception {
      User user = User.newBuilder().build();
      user.setName("Alice");
      user.setFavoriteNumber(12);
      addToCache(user);

      final HashMap<String,String> hardDrives
            = new HashMap<String, String>() {{put("STCD00502", "SEAGATE");put("STA045M", "SEAGATE");}};
      DeviceList deviceList = DeviceList.newBuilder().build();
      deviceList.setDevices(new ArrayList<Map<String, String>>() {{
         add(hardDrives);
      }});
      deviceList.setName("my devices");
      addToCache(deviceList);

      WebPage page = WebPage.newBuilder().build();
      page.setKey("http://www.test.com");
      Map<String,String> outlinks = new HashMap<>();
      outlinks.put("1", "http://www.example.com");
      page.setOutlinks(outlinks);
      addToCache(page);

      SearchManager sm = Search.getSearchManager(cache);

      Query luceneQuery = sm.buildQueryBuilderForClass(GenericData.Record.class)
            .get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()
            .ignoreAnalyzer()
            .matching("Alice")
            .createQuery();
      List<Object> list = sm.getQuery(luceneQuery).list();
      assertEquals(1, list.size());
      assertEquals(list.get(0), toGeneric(user));

      luceneQuery = NumericRangeQuery.newIntRange("favorite_number", 8, 0, 12, true, true);
      list = sm.getQuery(luceneQuery).list();
      assertEquals(1, list.size());
      assertEquals(list.get(0), toGeneric(user));

      luceneQuery = sm.buildQueryBuilderForClass(GenericData.Record.class)
            .get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()
            .ignoreAnalyzer()
            .matching("Bob")
            .createQuery();
      list = sm.getQuery(luceneQuery).list();
      assertEquals(0, list.size());

      luceneQuery = sm.buildQueryBuilderForClass(GenericData.Record.class)
            .get()
            .keyword()
            .onField("devices" + Support.DELIMITER+"0")
                  .ignoreFieldBridge()
                  .ignoreAnalyzer()
            .matching("STCD00502" + Support.DELIMITER + "SEAGATE")
            .createQuery();
      list = sm.getQuery(luceneQuery).list();
      assertEquals(1, list.size());
      assertEquals(list.get(0),toGeneric(deviceList));

      luceneQuery = sm.buildQueryBuilderForClass(GenericData.Record.class)
            .get()
            .keyword()
            .onField("outlinks")
                  .ignoreFieldBridge()
                  .ignoreAnalyzer()
                  .matching("1" + Support.DELIMITER+"http://www.example.com")
                  .createQuery();
      list = sm.getQuery(luceneQuery).list();
      assertEquals(1,list.size());
      assertEquals(list.get(0),toGeneric(page));
   }

   // helpers

   private void addToCache(SpecificRecord record) {
      cache.put(record.get(0),toGeneric(record));
   }

   private GenericData.Record toGeneric(SpecificRecord record){
      try{
         return (GenericData.Record) externalizer.objectFromByteBuffer(externalizer.objectToByteBuffer(record));
      } catch (IOException | ClassNotFoundException e) {
         e.printStackTrace();
      }
      return null;
   }

   private static class SimpleExternalizer extends AbstractMarshaller {

      private Map<String, Schema> knownSchema;
      private final SpecificRecord[] classList =
            {
                  new User(),
                  new DeviceList(),
                  new Employee(),
                  new WebPage()
            };


      public SimpleExternalizer(){
         knownSchema = new HashMap<>();
         for (SpecificRecord record : classList)
            knownSchema.put(record.getSchema().getFullName(),record.getSchema());
      }

      @Override
      protected DatumReader reader(String schemaName)
            throws InterruptedException, IOException, ClassNotFoundException {
         return new GenericDatumReader(knownSchema.get(schemaName));
      }
   }
}
