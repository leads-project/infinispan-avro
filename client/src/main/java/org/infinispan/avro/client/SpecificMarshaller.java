package org.infinispan.avro.client;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;

/**
 * @author Pierre Sutra
 */
public class SpecificMarshaller<T extends GenericContainer> extends AbstractMarshaller {

   private SpecificDatumReader<T> reader;

   public SpecificMarshaller(Class<T> c) {
      reader = new SpecificDatumReader<>(c);
   }

   @Override
   protected DatumReader reader(String schemaName)
         throws InterruptedException, IOException, ClassNotFoundException {
      return reader;
   }

}
