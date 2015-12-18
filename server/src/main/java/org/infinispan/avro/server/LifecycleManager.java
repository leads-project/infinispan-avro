package org.infinispan.avro.server;

import org.infinispan.avro.client.Support;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.*;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.expiration.impl.ExpirationInterceptor;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

/**
 * @author Pierre Sutra
 */
public class LifecycleManager extends AbstractModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.AVRO_VALUE_WRAPPER, new Externalizer());
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      MetadataManager.setInstance(new MetadataManager((DefaultCacheManager) cacheManager));
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
   }

   /**
    * Registers the remote value wrapper interceptor in the cache before it gets started.
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      if (!cacheName.equals(Support.AVRO_METADATA_CACHE_NAME)) {
         if (cfg.indexing().index().isEnabled() && !cfg.compatibility().enabled()) {
            log.infof("Registering RemoteAvroValueWrapperInterceptor for cache %s", cacheName);
            createRemoteIndexingInterceptor(cr, cfg);
         }
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      boolean remoteValueWrappingEnabled =
            configuration.indexing().index().isEnabled()
                  && !configuration.compatibility().enabled();
      if (!remoteValueWrappingEnabled) {
         if (verifyChainContainsInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find org.infinispan.avro.server.Interceptor "
                  + "registered in the InterceptorChain as indexing was disabled, but it was found");
         }
         return;
      }
      if (!verifyChainContainsInterceptor(cr)) {
         throw new IllegalStateException("It was expected to find org.infinispan.avro.server.interceptor "
               + "registered in the InterceptorChain but it wasn't found");
      }
   }

   private boolean verifyChainContainsInterceptor(ComponentRegistry cr) {
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      return interceptorChain.containsInterceptorType(Interceptor.class, true);
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      Configuration cfg = cr.getComponent(Configuration.class);
      removeRemoteIndexingInterceptorFromConfig(cfg);
   }

   // Helpers


   private void createRemoteIndexingInterceptor(ComponentRegistry cr, Configuration cfg) {
      Interceptor interceptor = cr.getComponent(Interceptor.class);
      if (interceptor == null) {
         interceptor = new Interceptor();

         // Interceptor registration not needed, core configuration handling
         // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
         InterceptorChain ic = cr.getComponent(InterceptorChain.class);
         ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);

         InterceptorConfigurationBuilder rightInterceptorBuilder = builder.customInterceptors().addInterceptor();
         rightInterceptorBuilder.interceptor(interceptor);
         rightInterceptorBuilder.after(ExpirationInterceptor.class);
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());

         if (ic != null) {
            ic.addInterceptorAfter(interceptor, ExpirationInterceptor.class);
            cr.registerComponent(interceptor, Interceptor.class);
            cr.registerComponent(interceptor, interceptor.getClass().getName(), true);
         }

      }
   }


   private void removeRemoteIndexingInterceptorFromConfig(Configuration cfg) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

      for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
         if (!(interceptorConfig.interceptor() instanceof Interceptor)) {
            customInterceptorsBuilder.addInterceptor().read(interceptorConfig);
         }
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }
}
