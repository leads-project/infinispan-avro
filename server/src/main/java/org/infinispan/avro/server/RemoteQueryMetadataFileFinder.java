package org.infinispan.avro.server;

import org.infinispan.factories.components.ModuleMetadataFileFinder;

/**
 * @author Pierre Sutra
 */
public class RemoteQueryMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-avro-server-component-metadata.dat";
   }
}
