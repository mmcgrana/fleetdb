package fleetdb;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.math.BigInteger;
import clojure.lang.ISeq;
import clojure.lang.IPersistentMap;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentVector;
import clojure.lang.LazilyPersistentVector;
import clojure.lang.IPersistentList;
import clojure.lang.LazySeq;
import clojure.lang.Seqable;
import clojure.lang.ArraySeq;
import clojure.lang.Keyword;
import clojure.lang.RT;

public class Serializer {
  private static final byte MAP_TYPE =         0;
  private static final byte VECTOR_TYPE =      1;
  private static final byte LIST_TYPE =        2;
  private static final byte KEYWORD_TYPE =     3;
  private static final byte STRING_TYPE =      4;
  private static final byte INTEGER_TYPE =     5;
  private static final byte LONG_TYPE =        6;
  private static final byte BIG_INTEGER_TYPE = 7;
  private static final byte DOUBLE_TYPE =      8;
  private static final byte BOOLEAN_TYPE =     9;
  private static final byte NIL_TYPE =         10;

  public static void serialize(DataOutputStream dos, Object obj) throws Exception {
    if (obj instanceof IPersistentMap) {
      IPersistentMap map = (IPersistentMap) obj;
      ISeq mSeq = map.seq();
      dos.writeByte(MAP_TYPE);
      dos.writeInt(map.count());
      while (mSeq != null) {
        IMapEntry me = (IMapEntry) mSeq.first();
        serialize(dos, me.key());
        serialize(dos, me.val());
        mSeq = mSeq.next();
      }

    } else if (obj instanceof IPersistentVector) {
      IPersistentVector vec = (IPersistentVector) obj;
      ISeq vSeq = vec.seq();
      dos.writeByte(VECTOR_TYPE);
      dos.writeInt(vec.count());
      while (vSeq != null) {
        serialize(dos, vSeq.first());
        vSeq = vSeq.next();
      }

    } else if ((obj instanceof IPersistentList) ||
               (obj instanceof LazySeq)) {
      ISeq lSeq = ((Seqable) obj).seq();
      int lCount = 0;
      ISeq lSeqTail = lSeq;
		  for(; lSeqTail != null; lSeqTail = lSeqTail.next()) {
		  	lCount++;
		  }
		  dos.writeByte(LIST_TYPE);
	    dos.writeInt(lCount);
	    while (lSeq != null) {
	      serialize(dos, lSeq.first());
	      lSeq = lSeq.next();
	    }

    } else if (obj instanceof Keyword) {
      Keyword kw = (Keyword) obj;
      byte[] bytes = kw.getName().getBytes();
      int byteSize = bytes.length;
      dos.writeByte(KEYWORD_TYPE);
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof String) {
      String str = (String) obj;
      byte[] bytes = str.getBytes();
      int byteSize = bytes.length;
      dos.writeByte(STRING_TYPE);
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof Integer) {
      dos.writeByte(INTEGER_TYPE);
      dos.writeInt((Integer) obj);

    } else if (obj instanceof Long) {
      dos.writeByte(LONG_TYPE);
      dos.writeLong((Long) obj);

    } else if (obj instanceof BigInteger) {
      byte[] bytes = ((BigInteger) obj).toByteArray();
      int byteSize = bytes.length;
      dos.writeByte(BIG_INTEGER_TYPE);
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof Double) {
      dos.writeByte(DOUBLE_TYPE);
      dos.writeDouble((Double) obj);

    } else if (obj instanceof Boolean) {
      dos.writeByte(BOOLEAN_TYPE);
      dos.writeBoolean((Boolean) obj);

    } else if (obj == null) {
      dos.writeByte(NIL_TYPE);

    } else {
      throw new Exception("Cannot serialize " + obj);
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

        case LIST_TYPE:
          int numLObjs = dis.readInt();
          Object[] lObjs = new Object[numLObjs];
          for (int i = 0; i < numLObjs; i++) {
            lObjs[i] = deserialize(dis, eofValue);
          }
          return ArraySeq.create(lObjs);
          
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
        
        case BIG_INTEGER_TYPE:
          int byteSize = dis.readInt();
          byte[] bytes = new byte[byteSize];
          dis.read(bytes, 0, byteSize);
          return new BigInteger(bytes);

        case DOUBLE_TYPE:
          return dis.readDouble();

        case BOOLEAN_TYPE:
          return dis.readBoolean();

        case NIL_TYPE:
          return null;

        default:
          throw new Exception("Cannot deserialize " + typeByte);
      }
    } catch (EOFException e) {
      return eofValue;
    }
  }
}