package fleetdb;

import clojure.lang.Keyword;
import clojure.lang.Numbers;
import clojure.lang.APersistentVector;
      
public class Compare {
  static final Keyword NEG_INF = Keyword.intern(null, "neg-inf");
  static final Keyword POS_INF = Keyword.intern(null, "pos-inf");  
  
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
    } else {
      return ((Comparable) a).compareTo((Comparable) b);
    }
  }
}