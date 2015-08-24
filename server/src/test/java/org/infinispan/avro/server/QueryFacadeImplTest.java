package org.infinispan.avro.server;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static org.testng.Assert.assertEquals;

/**
 * @author Pierre Sutra
 */
@Test(groups = "functional", testName = "org.infinispan.avro.server.QueryFacadeImplTest")
public class QueryFacadeImplTest {

   /**
    * Test there is exactly one loadable provider.
    */
   public void testProvider() {
      List<org.infinispan.server.core.QueryFacade> implementations = new ArrayList<>();
      for (org.infinispan.server.core.QueryFacade impl : ServiceLoader.load(org.infinispan.server.core.QueryFacade.class)) {
         implementations.add(impl);
      }

      assertEquals(1, implementations.size());
      assertEquals(QueryFacade.class, implementations.get(0).getClass());
   }
}
