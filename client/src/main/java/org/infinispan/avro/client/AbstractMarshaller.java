package org.infinispan.avro.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Pierre Sutra
 */
public abstract class AbstractMarshaller extends AbstractExternalizer implements Marshaller{

   private static final byte RECORD = 0;
   private static final byte SCHEMA = 1;
   private static final byte OTHER = 2;

   public AbstractMarshaller(){}

   protected abstract DatumReader reader(String schemaName)
         throws InterruptedException, IOException, ClassNotFoundException;

   // Write

   @Override
   public void writeObject(ObjectOutput output, Object o) throws IOException {
      byte[] out = objectToByteBuffer(o);
      output.writeObject(out);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      byte[] buf = objectToByteBuffer(o);
      return new ByteBufferImpl(buf, 0, buf.length);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return objectToByteBuffer(obj);
   }

   @Override
   public byte[] objectToByteBuffer(Object o) throws IOException{
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      if (o instanceof GenericContainer) {
         oos.write(RECORD);
         GenericContainer container = (GenericContainer) o;
         oos.writeObject((container.getSchema().getFullName()));
         DatumWriter<GenericContainer> datumWriter = new GenericDatumWriter<>(container.getSchema());
         BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
         datumWriter.write(container,encoder);
         encoder.flush();
      } else if (o instanceof Schema) {
         oos.write(SCHEMA);
         oos.writeUTF(o.toString());
      } else {
         oos.write(OTHER);
         oos.writeObject(o);
      }
      return baos.toByteArray();
   }

   // Read

   @Override
   public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer((byte[]) input.readObject());
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length)
         throws IOException, ClassNotFoundException {
      if (offset!=0||length!=buf.length) {
         objectFromByteBuffer(Arrays.copyOfRange(buf, offset, length));
      }
      return objectFromByteBuffer(buf);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      try {
         ByteArrayInputStream bais = new ByteArrayInputStream(buf);
         ObjectInputStream ois = new ObjectInputStream(bais);
         Byte header = ois.readByte();
         switch (header) {
         case RECORD:
            String schemaName = (String) ois.readObject();
            Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
            DatumReader reader = reader(schemaName);
            return reader.read(null, decoder);
         case SCHEMA:
            return Schema.parse(ois.readUTF());
         case OTHER:
            return ois.readObject();
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
      throw new IOException("cannot unmarshall");
   }

   // Others

   @Override
   public Set<Class<? extends Object>> getTypeClasses() {
      Set<Class<? extends Object>> set = new HashSet<>();
      set.add(GenericData.Record.class);
      set.add(Schema.class);
      set.add(Serializable.class);
      return set;
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return (o instanceof GenericData.Record) || (o instanceof Schema) || (o instanceof Serializable);
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return null;  // TODO: Customise this generated block
   }

}
