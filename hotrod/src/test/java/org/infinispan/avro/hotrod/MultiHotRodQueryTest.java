package org.infinispan.avro.hotrod;

import example.avro.Employee;
import org.infinispan.avro.client.Marshaller;
import org.infinispan.avro.client.Support;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.avro.hotrod.Utils.createEmployee1;
import static org.infinispan.avro.hotrod.Utils.createEmployee2;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Pierre Sutra
 */
@Test(testName = "org.infinispan.avro.hotrod.MultiHotRodQueryTest", groups = "functional")
public class MultiHotRodQueryTest extends MultiHotRodServersTest {

   protected RemoteCache<Integer, Employee> remoteCache0, remoteCache1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      builder.indexing().enable()
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createHotRodServers(2, builder);

      remoteCache0 = client(0).getCache();
      remoteCache1 = client(1).getCache();

      Support.registerSchema(client(0), Employee.getClassSchema());

      Thread.sleep(1000); // wait that the cluster forms

   }

   @Override
   protected RemoteCacheManager createClient(int i) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder
            = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host(server(i).getAddress().host()).port(server(i).getAddress().port());
      clientBuilder.marshaller(new Marshaller<Employee>(Employee.class));
      return new RemoteCacheManager(clientBuilder.build());
   }

   @Test
   public void testAttributeQuery() throws Exception {

      remoteCache0.put(1, createEmployee1());
      remoteCache0.put(2, createEmployee2());

      QueryFactory qf = Search.getQueryFactory(remoteCache1);

      Query query = qf.from(Employee.class)
            .having("ssn").eq("12357").toBuilder()
            .build();
      List<Employee> list = query.list();
      assertNotNull(list);
      assertEquals(2, list.size());

   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer()
            .host("localhost")
            .port(serverPort)
            .pingOnStartup(false);
      clientBuilder.marshaller(new Marshaller<>(Employee.class));
      return clientBuilder;
   }

}
