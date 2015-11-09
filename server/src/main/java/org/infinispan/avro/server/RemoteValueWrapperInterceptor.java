package org.infinispan.avro.server;

import org.apache.avro.generic.GenericData;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.compat.BaseTypeConverterInterceptor;

import java.io.IOException;
import java.util.Set;

/**
 * @author Pierre Sutra
 */
public class RemoteValueWrapperInterceptor<K,V> extends BaseTypeConverterInterceptor {

   private final ValueWrapperTypeConverter avroTypeConverter = new ValueWrapperTypeConverter();

   private final PassThroughTypeConverter passThroughTypeConverter = new PassThroughTypeConverter();

   @Override
   protected TypeConverter<Object, Object, Object, Object> determineTypeConverter(Set<Flag> flags) {
      return flags != null && flags.contains(Flag.OPERATION_HOTROD) ? avroTypeConverter : passThroughTypeConverter;
   }

   /**
    * A no-op converter.
    */
   private static class PassThroughTypeConverter implements TypeConverter<Object, Object, Object, Object> {

      @Override
      public Object boxKey(Object key) {
         return key;
      }

      @Override
      public Object boxValue(Object value) {
         return value;
      }

      @Override
      public Object unboxKey(Object target) {
         return target;
      }

      @Override
      public Object unboxValue(Object target) {
         return target;
      }

      @Override
      public boolean supportsInvocation(Flag flag) {
         return false;
      }

      @Override
      public void setMarshaller(Marshaller marshaller) {
      }
   }

   /**
    * A converter that wraps/unwraps the value (a byte[]) into a AvroValueWrapper.
    */
   private static class ValueWrapperTypeConverter extends PassThroughTypeConverter {

      Externalizer externalizer = new Externalizer();

      @Override
      public Object boxValue(Object value) {
         if (value instanceof byte[])
             try {
                 return externalizer.objectFromByteBuffer((byte[])value);
             } catch (IOException | ClassNotFoundException e) {
                 e.printStackTrace();
             }
          return value;
      }

      @Override
      public Object unboxValue(Object target) {
         if (target instanceof GenericData.Record)
             try {
                 return externalizer.objectToByteBuffer(target);
             } catch (IOException e) {
                 e.printStackTrace();
             }
          return target;
      }
   }
}