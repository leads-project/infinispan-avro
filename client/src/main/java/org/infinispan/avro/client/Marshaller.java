package org.infinispan.avro.client;

import org.apache.avro.generic.GenericContainer;

/**
 * @author Pierre Sutra
 */
public class Marshaller<T extends GenericContainer> extends SpecificMarshaller {

   public Marshaller(Class c) {
      super(c);
   }

}
