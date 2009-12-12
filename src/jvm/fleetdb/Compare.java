package fleetdb;

import clojure.lang.Keyword;
import clojure.lang.Numbers;
import clojure.lang.IPersistentVector;
      
public class Compare {
  static final Keyword NEG_INF = Keyword.intern(null, "neg-inf");
  static final Keyword POS_INF = Keyword.intern(null, "pos-inf");  
  
  @SuppressWarnings("unchecked")
  public static int compare(IPersistentVector a, IPersistentVector b) {
    if (a.count() != b.count()) {
    throw new IllegalArgumentException(
      "Cannot compare " + a.toString() + " and " + b.toString());
    }
	  for (int i = 0; i < a.count(); i++) {
	  	int c = compare(a.nth(i), b.nth(i));
	  	if (c != 0) {
	  		return c;
	  	}
	  }
	  return 0;
  }

  @SuppressWarnings("unchecked")
  public static int compare(Object a, Object b) {
    if (a == b) {
      return 0;
    } else if ((a == NEG_INF) || (b == POS_INF)) {
      return -1;
    } else if ((a == POS_INF) || (b == NEG_INF)) {
      return 1;
    } else if (a == null) {
      return -1;
    } else if (b == null) {
      return 1;
    } else if (a instanceof Number) {
      return Numbers.compare((Number) a, (Number) b);
    } else if (a instanceof String) {
      return ((String) a).compareTo((String) b);
    } else if (a instanceof Keyword) {
      return ((Keyword) a).compareTo((Keyword) b);
    } else if (a instanceof IPersistentVector ) {
      return compare((IPersistentVector) a, (IPersistentVector) b);
    } else {
      throw new IllegalArgumentException(
        "Cannot compare " + a.toString() + " and " + b.toString());
    }
  }
  
  public static boolean eq(Object a, Object b) {
    return compare(a, b) == 0;
  }
  
  public static boolean neq(Object a, Object b) {
    return compare(a, b) != 0;
  }
  
  public static boolean lt(Object a, Object b) {
    return compare(a, b) < 0;
  }
  
  public static boolean lte(Object a, Object b) {
    return compare(a, b) <= 0;
  }
  
  public static boolean gt(Object a, Object b) {
    return compare(a, b) > 0;
  }
  
  public static boolean gte(Object a, Object b) {
    return compare(a, b) >= 0;
  }
}