package org.infinispan.avro.server;

import org.apache.avro.Schema;
import org.infinispan.avro.client.Support;
import org.infinispan.avro.client.Request;
import org.infinispan.avro.client.Response;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Pierre Sutra
 */
public class MetadataManager {

   private static MetadataManager instance;

   private static final Log log = LogFactory.getLog(QueryFacade.class, Log.class);

   public static MetadataManager getInstance(){
      return instance;
   }
   public static void setInstance(MetadataManager instance){
         MetadataManager.instance = instance;
   }

   private DefaultCacheManager cacheManager;
   private ConcurrentMap<String, Schema> knownSchemas;
   private Marshaller marshaller;

   public MetadataManager(DefaultCacheManager cacheManager){
      Configuration cacheConfiguration = cacheManager.getCacheConfiguration(Support.AVRO_METADATA_CACHE_NAME);
      if (cacheConfiguration == null) {
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         CacheMode cacheMode =
               cacheManager.getDefaultCacheConfiguration().clustering().cacheMode().equals(CacheMode.LOCAL)
                     ? CacheMode.LOCAL : CacheMode.REPL_SYNC;
         cfg
               .transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(true).syncRollbackPhase(true)
               .persistence().addSingleFileStore().location(System.getProperty("java.io.tmpdir") + "/" + cacheManager.getNodeAddress()) // mandatory
               .locking().isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false)
               .clustering().cacheMode(cacheMode)
               .stateTransfer().fetchInMemoryState(true)
               .dataContainer().keyEquivalence(new ByteArrayEquivalence()); // for HotRod compatibility
         if (cacheMode.equals(CacheMode.REPL_SYNC)) cfg.clustering().stateTransfer().awaitInitialTransfer(true);
         cacheManager.defineConfiguration(Support.AVRO_METADATA_CACHE_NAME, cfg.build());
         this.cacheManager = cacheManager;
         this.marshaller= new Externalizer();
         this.knownSchemas = new ConcurrentHashMap<>();
         knownSchemas.put(Request.getClassSchema().getFullName(), Request.getClassSchema());
         knownSchemas.put(Response.getClassSchema().getFullName(), Response.getClassSchema());
      } else {
         throw new IllegalStateException("A configuration for "+ Support.AVRO_METADATA_CACHE_NAME
               +", but it should not be defined by the user.");
      }
   }

   public Schema retrieveSchema(String name) throws IOException, InterruptedException, ClassNotFoundException {
      if (!knownSchemas.containsKey(name)) {
         byte[] key = marshaller.objectToByteBuffer(name);
         byte[] value = (byte[]) cacheManager.getCache(Support.AVRO_METADATA_CACHE_NAME).get(key);
         if (value==null)
            throw new IOException(name+" not found in the metadata cache");
         Schema schema = (Schema) marshaller.objectFromByteBuffer(value);
         knownSchemas.put(name, schema);
         log.info("adding schama "+name+" to metadata cache");
      }
      return knownSchemas.get(name);
   }

}
