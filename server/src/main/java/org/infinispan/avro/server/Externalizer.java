package org.infinispan.avro.server;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.infinispan.avro.client.AbstractMarshaller;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Pierre Sutra
  */
public class Externalizer extends AbstractMarshaller {

   private static Externalizer instance = new Externalizer();
   public static Externalizer getInstance() {
      return instance;
   }


   private Externalizer(){}


   // Others

   protected DatumReader reader(String schemaName)
         throws InterruptedException, IOException, ClassNotFoundException {
      Schema schema = MetadataManager.getInstance().retrieveSchema(schemaName);
      return new GenericDatumReader<>(schema);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return (o instanceof GenericData.Record) || (o instanceof Schema) || (o instanceof Serializable);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.AVRO_VALUE_WRAPPER;
   }

}
