package humboldt;

import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;


public class BinaryComparator<K, V> extends WritableComparator implements Configurable {
  public static String LEFT_OFFSET_PROPERTY_NAME = "humboldt.binarycomparator.left.offset";
  public static String RIGHT_OFFSET_PROPERTY_NAME = "humboldt.binarycomparator.right.offset";

  private Configuration conf;
  private int leftOffset;
  private int rightOffset;

  public static void setOffsets(Configuration conf, int left, int right) {
    conf.setInt(LEFT_OFFSET_PROPERTY_NAME, left);
    conf.setInt(RIGHT_OFFSET_PROPERTY_NAME, right);
  }

  public static void setLeftOffset(Configuration conf, int offset) {
    conf.setInt(LEFT_OFFSET_PROPERTY_NAME, offset);
  }

  public static void setRightOffset(Configuration conf, int offset) {
    conf.setInt(RIGHT_OFFSET_PROPERTY_NAME, offset);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    leftOffset = conf.getInt(LEFT_OFFSET_PROPERTY_NAME, 0);
    rightOffset = conf.getInt(RIGHT_OFFSET_PROPERTY_NAME, -1);
  }

  public Configuration getConf() {
    return conf;
  }

  public int compare(WritableComparable w1, WritableComparable w2) {
    BinaryComparable bc1 = (BinaryComparable) w1;
    BinaryComparable bc2 = (BinaryComparable) w2;
    return compare(bc1.getBytes(), 0, bc1.getLength(),
                   bc2.getBytes(), 0, bc2.getLength());
  }

  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int b1LeftIndex = (leftOffset + l1) % l1;
    int b1RightIndex = (rightOffset + l1) % l1;
    int b2LeftIndex = (leftOffset + l2) % l2;
    int b2RightIndex = (rightOffset + l2) % l2;
    return WritableComparator.compareBytes(b1, b1LeftIndex, b1RightIndex - b1LeftIndex + 1,
                                           b2, b2LeftIndex, b2RightIndex - b2LeftIndex + 1);
  }
}
