package fleetdb;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;
import java.nio.charset.Charset;
import java.math.BigInteger;
import clojure.lang.ISeq;
import clojure.lang.IPersistentMap;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentVector;
import clojure.lang.LazilyPersistentVector;
import clojure.lang.IPersistentList;
import clojure.lang.PersistentList;
import clojure.lang.Seqable;
import clojure.lang.ArraySeq;
import clojure.lang.Symbol;
import clojure.lang.Keyword;
import clojure.lang.RT;

public class Bert {
  private static final int MAGIC = 131;
  
  private static final int SMALL_INT = 97;
  private static final int INT = 98;
  private static final int SMALL_BIGNUM = 110;
  private static final int LARGE_BIGNUM = 111;
  private static final int FLOAT = 99;
  private static final int ATOM = 100;
  private static final int SMALL_TUPLE = 104;
  private static final int LARGE_TUPLE = 105;
  private static final int NIL = 106;
  private static final int STRING = 107;
  private static final int LIST = 108;
  private static final int BIN = 109;
  
  private static final int MAX_INT = 134217727;
  private static final int MIN_INT = -134217727;
  
  private static final Keyword BERT =     Keyword.intern("bert");
  private static final Keyword NIL_BERT = Keyword.intern("nil");
  private static final Keyword TRUE =     Keyword.intern("true");
  private static final Keyword FALSE =    Keyword.intern("false");
  private static final Keyword DICT =     Keyword.intern("dict");
  
  private static final BigInteger BIG_256 =       new BigInteger("256");
  private static final BigInteger BIG_LONG_MIN =  BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX =  BigInteger.valueOf(Long.MAX_VALUE);
  private static final Charset    UTF_8 = Charset.forName("UTF-8");

  public static void encode(DataOutputStream dos, Object obj) throws Exception {
    dos.writeByte(MAGIC);
    encodeWithoutMagic(dos, obj);
  }
  
  private static void encodeWithoutMagic(DataOutputStream dos, Object obj) throws Exception {
    if (obj instanceof Keyword) {
      encodeKeyword(dos, (Keyword) obj);
    } else if (obj instanceof Symbol) {
      encodeSymbol(dos, (Symbol) obj);
    } else if (obj instanceof String) {
      encodeString(dos, (String) obj);
    } else if (obj instanceof Integer) {
      encodeInteger(dos, (Integer) obj);
    } else if (obj instanceof Long) {
      encodeLong(dos, (Long) obj);
    } else if (obj instanceof BigInteger) {
      encodeBigInteger(dos, (BigInteger) obj);
    } else if (obj instanceof Double) {
      encodeDouble(dos, (Double) obj);
    } else if (obj instanceof Boolean) {
      encodeBoolean(dos, (Boolean) obj);
    } else if (obj == null) {
      encodeNil(dos);
    } else if (obj instanceof IPersistentMap) {
      encodeMap(dos, (IPersistentMap) obj);
    } else if (obj instanceof IPersistentVector) {
      encodeVector(dos, (IPersistentVector) obj);
    } else if ((obj instanceof IPersistentList) || (obj instanceof ISeq)) {
      encodeSeq(dos, ((Seqable) obj).seq(), false);
    } else {
      throw new Exception("Cannot encode " + obj);
    }
  }
  
  private static void encodeSymbol(DataOutputStream dos, Symbol sym) throws Exception {
    byte[] bytes = sym.getName().getBytes(UTF_8);
    int len = bytes.length;
    dos.writeByte(ATOM);
    dos.writeShort(len);
    dos.write(bytes);
  }
  
  private static void encodeKeyword(DataOutputStream dos, Keyword keyword) throws Exception {
    encodeSymbol(dos, keyword.sym);
  }

  private static void encodeInteger(DataOutputStream dos, int i) throws Exception {
    if ((i >= 0) && (i < 256)) {
      dos.writeByte(SMALL_INT);
      dos.writeByte(i);
    } else if ((i <= MAX_INT) && (i >= MIN_INT)) {
      dos.writeByte(INT);
      dos.writeInt(i);
    } else {
      encodeLong(dos, (long) i);
    }
  }
  
  private static void encodeLong(DataOutputStream dos, Long l) throws Exception {
    dos.writeByte(SMALL_BIGNUM);
    dos.writeByte((int) Math.ceil((64 - Long.numberOfLeadingZeros(l)) / 8.0));
    dos.writeByte(l >= 0 ? 0 : 1);
    long num = Math.abs(l);
    while (num != 0) {
      long rem = num & 255;
      dos.writeByte((int) rem);
      num = num >> 8;
    }
  }
  
  private static void encodeBigInteger(DataOutputStream dos, BigInteger bi) throws Exception {
    byte[] bytes = bi.abs().toByteArray();
    int len = bytes.length;
    if (len <= 256) {
      dos.writeByte(SMALL_BIGNUM);
      dos.writeByte(len);
    } else {
      dos.writeByte(LARGE_BIGNUM);
      dos.writeInt(len);
    }
    dos.writeByte((bi.signum() >= 0) ? 0 : 1);
    reverse(bytes);
    dos.write(bytes);
  }

  private static void encodeDouble(DataOutputStream dos, Double d) throws Exception {
    byte[] bytes = String.format("%15.15e", d).getBytes();
    byte[] padded = new byte[31];
    int fullLen = bytes.length;
    System.arraycopy(bytes, 0, padded, 0, fullLen);
    for (int i = fullLen; i < 31; i++) {
      padded[i] = 0;
    }
    dos.writeByte(FLOAT);
    dos.write(padded, 0, 31);
  }
  
  private static void encodeString(DataOutputStream dos, String str) throws Exception {
    byte[] bytes = str.getBytes(UTF_8);
    int len = bytes.length;
    dos.writeByte(BIN);
    dos.writeInt(len);
    dos.write(bytes);
  }
  
  private static void encodeBoolean(DataOutputStream dos, boolean b) throws Exception {
    dos.writeByte(SMALL_TUPLE);
    dos.writeByte(2);
    encodeKeyword(dos, BERT);
    encodeKeyword(dos, b ? TRUE : FALSE);
  }

  private static void encodeNil(DataOutputStream dos) throws Exception {
    dos.writeByte(SMALL_TUPLE);
    dos.writeByte(2);
    encodeKeyword(dos, BERT);
    encodeKeyword(dos, NIL_BERT);
  }
  
  private static void encodeMap(DataOutputStream dos, IPersistentMap map) throws Exception {
    dos.writeByte(SMALL_TUPLE);
    dos.writeByte(3);
    encodeKeyword(dos, BERT);
    encodeKeyword(dos, DICT);
    encodeSeq(dos, map.seq(), true);
  }

  private static void encodeVector(DataOutputStream dos, IPersistentVector vec) throws Exception {
    int len = vec.count();
    if (len < 256) {
      dos.writeByte(SMALL_TUPLE);
      dos.writeByte(len);
    } else {
      dos.writeByte(LARGE_TUPLE);
      dos.writeInt(len);
    }
    for (int i = 0; i < len; i++) {
      encodeWithoutMagic(dos, vec.nth(i));  
    }
  }
  
  private static void encodeSeq(DataOutputStream dos, ISeq seq, boolean mapSeq) throws Exception {
    if (seq != null) {
      int len = seq.count();
      dos.writeByte(LIST);
      dos.writeInt(len);
      while (seq != null) {
        if (mapSeq) {
          encodeVector(dos, (IPersistentVector) seq.first());
        } else {
          encodeWithoutMagic(dos, seq.first());
        }
	      seq = seq.next();
	    }
    }
    dos.writeByte(NIL);
  }
  
  public static Object decode(DataInputStream dis, Object eofVal) throws Exception {
    try {
      int magic = unsignByte(dis.readByte());
      if (magic != MAGIC) {
        throw new Exception("Unrecognized magic: " + magic);
      } else {
        return decodeWithoutMagic(dis);
      }
    } catch (EOFException e) {
      return eofVal;
    }
  }
  
  private static Object decodeWithoutMagic(DataInputStream dis) throws Exception {
    int termByte = dis.readByte();
    switch(termByte) {
      case ATOM:
        return decodeAtom(dis);
      case BIN:
        return decodeBin(dis);
      case SMALL_INT:
        return decodeSmallInt(dis);
      case INT:
        return decodeInt(dis);
      case SMALL_BIGNUM:
        return decodeSmallBignum(dis);
      case LARGE_BIGNUM:
        return decodeLargeBignum(dis);
      case FLOAT:
        return decodeFloat(dis);
      case SMALL_TUPLE:
        return decodeSmallTuple(dis);
      case LARGE_TUPLE:
        return decodeLargeTuple(dis);
      case NIL:
        return PersistentList.EMPTY;
      case LIST:
        return decodeSeq(dis);
      default:
        throw new Exception("Cannot decode " + termByte);
    }
  }
  
  private static Keyword decodeAtom(DataInputStream dis) throws Exception {
    int len = dis.readShort();
    byte[] bytes = new byte[len];
    dis.read(bytes);
    return Keyword.intern(new String(bytes, UTF_8));
  }
  
  private static String decodeBin(DataInputStream dis) throws Exception {
    int len = dis.readInt();
    byte[] bytes = new byte[len];
    dis.read(bytes);
    return new String(bytes, UTF_8);
  }

  private static Integer decodeSmallInt(DataInputStream dis) throws Exception {
    return unsignByte(dis.readByte());
  }
  
  private static Integer decodeInt(DataInputStream dis) throws Exception {
    return dis.readInt();
  }
  
  private static Object decodeSmallBignum(DataInputStream dis) throws Exception {
    int size = unsignByte(dis.readByte());
    byte sign = dis.readByte();
    BigInteger bigNum = decodeBignum(dis, size, sign);
    if ((bigNum.compareTo(BIG_LONG_MIN) >= 0) &&
        (bigNum.compareTo(BIG_LONG_MAX) <= 0)) {
      return bigNum.longValue();
    } else {
      return bigNum;
    }
  }
    
  private static BigInteger decodeLargeBignum(DataInputStream dis) throws Exception {
    int size = dis.readInt();
    byte sign = dis.readByte();
    return decodeBignum(dis, size, sign);
  }

  private static BigInteger decodeBignum(DataInputStream dis, int size, byte sign) throws Exception {
    byte[] bytes = new byte[Math.max(size, 8)];
    dis.read(bytes, 0, size);
    reverse(bytes);
    BigInteger abs = new BigInteger(bytes);
    return (sign == 1) ? abs.negate() : abs;
  }
  
  private static Double decodeFloat(DataInputStream dis) throws Exception {
    byte[] bytes = new byte[31];
    dis.read(bytes);
    return new Double(new String(bytes));
  }
  
  private static Object decodeSmallTuple(DataInputStream dis) throws Exception {
    int len = unsignByte(dis.readByte());
    if (len == 0) {
      return LazilyPersistentVector.createOwning();
    } else {
      Object first = decodeWithoutMagic(dis);
      if (first.equals(BERT)) {
        Object second = decodeWithoutMagic(dis);
        if (second.equals(TRUE)) {
          return Boolean.TRUE;
        } else if (second.equals(FALSE)) {
          return Boolean.FALSE;
        } else if (second.equals(NIL_BERT)) {
          return null;
        } else if (second.equals(DICT)) {
          return decodeDict(dis);
        } else {
          Object[] objs = new Object[len];
          objs[0] = first;
          objs[1] = second;
          if (len > 2) {
            for (int i = 2; i < len; i++) {
              objs[i] = decodeWithoutMagic(dis);
            }
          }
          return LazilyPersistentVector.createOwning(objs);
        }
      } else {
        Object[] objs = new Object[len];
        objs[0] = first;
        for (int i = 1; i < len; i++) {
          objs[i] = decodeWithoutMagic(dis);
        }
        return LazilyPersistentVector.createOwning(objs);
      }
    }
  }
  
  private static IPersistentMap decodeDict(DataInputStream dis) throws Exception {
    int list_byte = dis.readByte();
    if (list_byte == NIL) {
      return RT.map();
    } else if (list_byte == LIST) {
      int pairs = dis.readInt();
      Object[] objs = new Object[pairs * 2];
      for (int p = 0; p < pairs; p++) {
        dis.readByte();
        dis.readByte();
        objs[(p * 2)] =     decodeWithoutMagic(dis);
        objs[(p * 2) + 1] = decodeWithoutMagic(dis);
      }
      dis.readByte();
      return RT.map(objs);
    } else {
      throw new Exception("Unexpected byte: " + list_byte);
    }
  }
  
  private static IPersistentVector decodeLargeTuple(DataInputStream dis) throws Exception {
    int len = dis.readInt();
    Object[] objs = new Object[len];
    for (int i = 0; i < len; i++) {
      objs[i] = decodeWithoutMagic(dis);
    }
    return LazilyPersistentVector.createOwning(objs);
  }

  private static ISeq decodeSeq(DataInputStream dis) throws Exception {
    int len = dis.readInt();
    Object objs[] = new Object[len];
    for (int i = 0; i < len; i++) {
      objs[i] = decodeWithoutMagic(dis);
    }
    int nil = dis.readByte();
    if (nil == NIL) {
      return ArraySeq.create(objs);
    } else {
      throw new Exception("Expected NIL at end of list, got: " + nil);
    }
  }
  
  private static int unsignByte(byte b) throws Exception {
    return ((int) b) & 0xFF;
  }
  
  private static void reverse(byte[] b) {
    int left  = 0;
    int right = b.length-1;
    while (left < right) {
       byte temp = b[left]; 
       b[left]  = b[right]; 
       b[right] = temp;
       left++;
       right--;
    }
  }
}