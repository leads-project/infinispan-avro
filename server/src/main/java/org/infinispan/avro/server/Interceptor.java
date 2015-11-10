package org.infinispan.avro.server;

import org.apache.avro.generic.GenericData;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.compat.TypeConverter;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.io.IOException;

/**
 * @author Pierre Sutra
 */
public class Interceptor extends CommandInterceptor {

   private final ValueWrapperTypeConverter avroTypeConverter = new ValueWrapperTypeConverter();

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object ret = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      command.setKey(avroTypeConverter.unboxKey(key));
      command.setValue(avroTypeConverter.unboxValue(command.getValue()));
      return ret;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object ret = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      command.setKey(avroTypeConverter.unboxKey(key));
      command.setValue(avroTypeConverter.unboxValue(command.getValue()));
      return ret;
   }


   // Helpers

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

   private static class ValueWrapperTypeConverter extends PassThroughTypeConverter {

      Externalizer externalizer = new Externalizer();

      @Override
      public Object boxValue(Object value) {
         if (value instanceof GenericData.Record)
            try {
               return externalizer.objectToByteBuffer(value);
            } catch (IOException e) {
               e.printStackTrace();
            }
         return value;
      }

      @Override
      public Object unboxValue(Object value) {
         if (value instanceof byte[])
            try {
               return externalizer.objectFromByteBuffer((byte[]) value);
            } catch (IOException | ClassNotFoundException e) {
               e.printStackTrace();
            }
         return value;
      }
   }

}