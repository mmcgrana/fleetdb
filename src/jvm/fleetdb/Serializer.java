package fleetdb;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;

import clojure.lang.ISeq;
import clojure.lang.IPersistentMap;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentVector;
import clojure.lang.LazilyPersistentVector;
import clojure.lang.Keyword;
import clojure.lang.RT;

public class Serializer {
  private static final byte MAP_TYPE =     0;
  private static final byte VECTOR_TYPE =  1;
  private static final byte KEYWORD_TYPE = 2;
  private static final byte STRING_TYPE =  3;
  private static final byte INTEGER_TYPE = 4;
  private static final byte LONG_TYPE =    5;
  private static final byte DOUBLE_TYPE =  6;
  private static final byte BOOLEAN_TYPE = 7;
  private static final byte NIL_TYPE =     8;

  public static void serialize(DataOutputStream dos, Object obj) throws Exception {
    if (obj instanceof IPersistentMap) {
      dos.writeByte(MAP_TYPE);
      IPersistentMap map = (IPersistentMap) obj;
      dos.writeInt(map.count());
      ISeq mSeq = map.seq();
      while (mSeq != null) {
        IMapEntry me = (IMapEntry) mSeq.first();
        serialize(dos, me.key());
        serialize(dos, me.val());
        mSeq = mSeq.next();
      }

    } else if (obj instanceof IPersistentVector) {
      dos.writeByte(VECTOR_TYPE);
      IPersistentVector vec = (IPersistentVector) obj;
      dos.writeInt(vec.count());
      ISeq vSeq = vec.seq();
      while (vSeq != null) {
        serialize(dos, vSeq.first());
        vSeq = vSeq.next();
      }

    } else if (obj instanceof Keyword) {
      dos.writeByte(KEYWORD_TYPE);
      Keyword kw = (Keyword) obj;
      byte[] bytes = kw.getName().getBytes();
      int byteSize = bytes.length;
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof String) {
      dos.writeByte(STRING_TYPE);
      String str = (String) obj;
      byte[] bytes = str.getBytes();
      int byteSize = bytes.length;
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof Integer) {
      dos.writeByte(INTEGER_TYPE);
      dos.writeInt((Integer) obj);

    } else if (obj instanceof Long) {
      dos.writeByte(LONG_TYPE);
      dos.writeLong((Long) obj);

    } else if (obj instanceof Double) {
      dos.writeByte(DOUBLE_TYPE);
      dos.writeDouble((Double) obj);

    } else if (obj instanceof Boolean) {
      dos.writeByte(BOOLEAN_TYPE);
      dos.writeBoolean((Boolean) obj);

    } else if (obj == null) {
      dos.writeByte(NIL_TYPE);

    } else {
      throw new Exception("Can not serialize " + obj);
    }
  }

  public static Object deserialize(DataInputStream dis, Object eofValue) throws Exception {
    try {
      byte typeByte = dis.readByte();
      switch (typeByte) {
        case MAP_TYPE:
          int numMObjs = dis.readInt() * 2;
          Object[] mObjs = new Object[numMObjs];
          for (int i = 0; i < numMObjs; i++) {
            mObjs[i] = deserialize(dis, eofValue);
          }
          return RT.map(mObjs);

        case VECTOR_TYPE:
          int numVObjs = dis.readInt();
          Object[] vObjs = new Object[numVObjs];
          for (int i = 0; i < numVObjs; i++) {
            vObjs[i] = deserialize(dis, eofValue);
          }
          return LazilyPersistentVector.createOwning(vObjs);

        case KEYWORD_TYPE:
          int keyByteSize = dis.readInt();
          byte[] keyBytes = new byte[keyByteSize];
          dis.read(keyBytes, 0, keyByteSize);
          return Keyword.intern(new String(keyBytes));

        case STRING_TYPE:
          int strByteSize = dis.readInt();
          byte[] strBytes = new byte[strByteSize];
          dis.read(strBytes, 0, strByteSize);
          return new String(strBytes);

        case INTEGER_TYPE:
          return dis.readInt();

        case LONG_TYPE:
          return dis.readLong();

        case DOUBLE_TYPE:
          return dis.readDouble();

        case BOOLEAN_TYPE:
          return dis.readBoolean();

        case NIL_TYPE:
          return null;

        default:
          throw new Exception("Can not deserialize " + typeByte);
      }
    } catch (EOFException e) {
      return eofValue;
    }
  }
}