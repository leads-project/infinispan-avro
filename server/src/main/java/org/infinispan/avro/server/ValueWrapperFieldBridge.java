package org.infinispan.avro.server;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.lucene.document.*;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;
import org.hibernate.search.bridge.TwoWayFieldBridge;
import org.hibernate.search.bridge.builtin.ArrayBridge;
import org.hibernate.search.bridge.builtin.NumericFieldBridge;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.hibernate.search.bridge.builtin.impl.TwoWayString2FieldBridgeAdaptor;
import org.infinispan.commons.logging.Log;
import org.infinispan.avro.client.Support;
import org.infinispan.commons.CacheException;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Pierre Sutra
 */
public class ValueWrapperFieldBridge implements TwoWayFieldBridge{

   private static final Log log = LogFactory.getLog(ValueWrapperFieldBridge.class, Log.class);

   @Override
   public void set(String name, Object value, Document document, LuceneOptions luceneOptions) {
      if (name.contains(Support.DELIMITER))
         throw new CacheException("Name cannot contains delimiter \""+ Support.DELIMITER+"\"");
      GenericData.Record record = (GenericData.Record) value;
      Schema schema = record.getSchema();
      add(document, schema.getType(), "", value, schema);
   }

   @Override
   public Object get(String name, Document document) {
      if (document.get(name).equals(Schema.Type.NULL.toString()))
         return null;
      return document.get(name);
   }

   @Override
   public String objectToString(Object object) {
      if (object==null)
         return Schema.Type.NULL.toString();
      return object.toString();
   }

   // Helpers

   private void add(Document document, Schema.Type type, String name, Object value, Schema schema) {

      if (value==null) {
         log.trace("ignoring null value");
         return;
      }

      switch (type) {
      case BYTES:
      case NULL:
         log.trace("type "+type+" not indexed");
         break;
      case INT:
         add(document, new IntField(name, (Integer) value, Field.Store.NO));
         break;
      case LONG:
         add(document, new LongField(name, (Long) value, Field.Store.NO));
         break;
      case FLOAT:
         add(document, new FloatField(name, (Float) value, Field.Store.NO));
         break;
      case DOUBLE:
         add(document, new DoubleField(name, (Double) value, Field.Store.NO));
         break;
      case UNION:
         for (Schema subSchema : schema.getTypes()) {
            try {
               add(document, subSchema.getType(), name, value, subSchema);
            }catch(Exception e){
               // ignore, borderline approach yet the only one.
            }
         }
         break;
      case RECORD:
         GenericData.Record record = (GenericData.Record) value;
         for (Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.name();
            Schema subSchema = field.schema();
            add(
                  document,
                  subSchema.getType(),
                  (name.isEmpty() ? fieldName : name + Support.DELIMITER + fieldName),
                  record.get(fieldName),
                  subSchema);
         }
         break;
      case MAP:
         Map<?, ?> map = (Map) value;
         for (Object k : map.keySet()) {
            Schema subSchema = schema.getValueType();
            if (map.get(k)!=null)
               add(document,
                     new StringField(
                           name,
                           k+ Support.DELIMITER+map.get(k).toString(),
                           Field.Store.NO)); // for compatibility purposes
         }
         break;
      case ARRAY:
         GenericData.Array array = (GenericData.Array) value;
         for (int i =0; i < array.size(); i++) {
            Schema subSchema = schema.getElementType();
            add(
                  document,
                  subSchema.getType(),
                  name + Support.DELIMITER + i,
                  array.get(i),
                  subSchema);
         }
         break;
      case ENUM:
      case FIXED:
      case STRING:
      case BOOLEAN:
         add(
               document,
               new StringField(
                     name,
                     value.toString(),
                     Field.Store.NO));
         break;
      default:
         throw new CacheException("Unreachable code");
      }

   }

   /**
    * By default field whose value is greater than 1000 are not indexed.
    * @param document
    * @param field
    */
   private void add(Document document, Field field) {
      if (field.stringValue().length()>1000) {
         log.trace("field "+field.name()+" too long; not indexed");
      } else {
         document.add(field);
         log.trace("adding "+field.name()+"["+field.getClass().getSimpleName()+"]: " + field);
      }
   }

   public static FieldBridge retrieveFieldBridge(String fieldName, Schema schema) {
      List<String> path = new ArrayList<>();
      path.add(schema.getName());
      for(String p : fieldName.split(Support.DELIMITER_REGEX))
         path.add(p);
      return retrieveFieldBridge(
            schema.getType(),
            schema,
            path.toArray(new String[]{}));
   }

   public static TwoWayFieldBridge retrieveFieldBridge(Schema.Type type, Schema schema, String[] path) {
      switch (type) {
      case BYTES:
      case NULL:
          log.trace("type " + type + " not indexed");
      case INT:
         return NumericFieldBridge.INT_FIELD_BRIDGE;
      case LONG:
         return NumericFieldBridge.LONG_FIELD_BRIDGE;
      case FLOAT:
         return NumericFieldBridge.FLOAT_FIELD_BRIDGE;
      case DOUBLE:
         return NumericFieldBridge.DOUBLE_FIELD_BRIDGE;
      case UNION:
         for (Schema subSchema : schema.getTypes())
            if (!subSchema.getType().equals(Schema.Type.NULL))
               return retrieveFieldBridge(subSchema.getType(), subSchema, path);
         break;
      case RECORD:
         String[] subPath = Arrays.copyOfRange(path,1,path.length);
         for (Schema.Field field : schema.getFields()) {
            if (field.name().equals(path[1]))
               return retrieveFieldBridge(
                     field.schema().getType(),
                     field.schema(),
                     subPath);
         }
         break;
      case MAP:
         Schema subSchema = schema.getValueType();
         return retrieveFieldBridge(
               subSchema.getType(),
               subSchema,
               Arrays.copyOfRange(path,1,path.length));
      case ARRAY:
         subSchema = schema.getElementType();
         return new TwoWayArrayBridge(
               retrieveFieldBridge(
                     subSchema.getType(),
                     subSchema,
                     path));
      case ENUM:
      case FIXED:
      case STRING:
      case BOOLEAN:
         return new TwoWayString2FieldBridgeAdaptor(StringBridge.INSTANCE);
      }
      throw new CacheException("Unable to find "+Arrays.toString(path)+" having type "+type);
   }

   public static class TwoWayArrayBridge extends ArrayBridge implements TwoWayFieldBridge {

      private TwoWayFieldBridge bridge;

      /**
       * @param bridge the {@link FieldBridge} used for each entry of the array
       */
      public TwoWayArrayBridge(TwoWayFieldBridge bridge) {
         super(bridge);
         this.bridge = bridge;
      }

      @Override
      public Object get(String name, Document document) {
         List<Object> array = new ArrayList<>();
         for (int i=0;; i++) {
            Object o = bridge.get(name+ Support.DELIMITER+i, document);
            if (o!=null)
               array.add(o);
            else
               break;
         }
         return document.getFields(name);
      }

      @Override
      public String objectToString(Object object) {
         throw new CacheException("NYI.");
      }
   }

}
