package redis.util;

import redis.util.BytesKey;

/**
* Created with IntelliJ IDEA.
* User: spullara
* Date: 7/22/12
* Time: 4:05 PM
* To change this template use File | Settings | File Templates.
*/
public class ZSetEntry implements Comparable<redis.util.ZSetEntry> {
  private final BytesKey key;
  private double score;

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof redis.util.ZSetEntry && key.equals(((redis.util.ZSetEntry) obj).key);
  }

  public ZSetEntry(BytesKey key, double score) {
    this.key = key;
    this.score = score;
  }

  @Override
  public int compareTo(redis.util.ZSetEntry o) {
    double diff = score - o.score;
    return diff < 0 ? -1 : diff == 0 ? 0 : 1;
  }

  public BytesKey getKey() {
    return key;
  }

  public double getScore() {
    return score;
  }

  public double increment(double value) {
    return score += value;
  }
}
