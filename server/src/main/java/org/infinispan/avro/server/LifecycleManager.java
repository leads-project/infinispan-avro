package org.infinispan.avro.server;

import org.infinispan.avro.client.Support;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.*;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.interceptors.BatchingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.compat.BaseTypeConverterInterceptor;
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

   private void createRemoteIndexingInterceptor(ComponentRegistry cr, Configuration cfg) {
      BaseTypeConverterInterceptor wrapperInterceptor = cr.getComponent(RemoteValueWrapperInterceptor.class);
      if (wrapperInterceptor == null) {
         wrapperInterceptor = new RemoteValueWrapperInterceptor();

         // Interceptor registration not needed, core configuration handling
         // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
         InterceptorChain ic = cr.getComponent(InterceptorChain.class);

         ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);
         InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
         interceptorBuilder.interceptor(wrapperInterceptor);

         if (cfg.invocationBatching().enabled()) {
            if (ic != null) ic.addInterceptorAfter(wrapperInterceptor, BatchingInterceptor.class);
            interceptorBuilder.after(BatchingInterceptor.class);
         } else {
            if (ic != null) ic.addInterceptorAfter(wrapperInterceptor, InvocationContextInterceptor.class);
            interceptorBuilder.after(InvocationContextInterceptor.class);
         }
         if (ic != null) {
            cr.registerComponent(wrapperInterceptor, RemoteValueWrapperInterceptor.class);
            cr.registerComponent(wrapperInterceptor, wrapperInterceptor.getClass().getName(), true);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      boolean remoteValueWrappingEnabled = configuration.indexing().index().isEnabled() && !configuration.compatibility().enabled();
      if (!remoteValueWrappingEnabled) {
         if (verifyChainContainsRemoteAvroValueWrapperInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the RemoteAvroValueWrapperInterceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }
         return;
      }
      if (!verifyChainContainsRemoteAvroValueWrapperInterceptor(cr)) {
         throw new IllegalStateException("It was expected to find the RemoteAvroValueWrapperInterceptor registered in the InterceptorChain but it wasn't found");
      }
   }

   private boolean verifyChainContainsRemoteAvroValueWrapperInterceptor(ComponentRegistry cr) {
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      return interceptorChain.containsInterceptorType(RemoteValueWrapperInterceptor.class, true);
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      Configuration cfg = cr.getComponent(Configuration.class);
      removeRemoteIndexingInterceptorFromConfig(cfg);
   }

   private void removeRemoteIndexingInterceptorFromConfig(Configuration cfg) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

      for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
         if (!(interceptorConfig.interceptor() instanceof RemoteValueWrapperInterceptor)) {
            customInterceptorsBuilder.addInterceptor().read(interceptorConfig);
         }
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }
}
