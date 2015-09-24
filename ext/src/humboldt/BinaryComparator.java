package humboldt;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;


public class BinaryComparator<K, V> extends WritableComparator implements Configurable {
  public static String LEFT_OFFSET_PROPERTY_NAME = "humboldt.binarycomparator.left.offset";
  public static String RIGHT_OFFSET_PROPERTY_NAME = "humboldt.binarycomparator.right.offset";

  private Configuration conf;
  private Class<? extends BinaryComparable> keyClass;
  private int leftOffset;
  private int rightOffset;

  public BinaryComparator() {
    super(null);
  }

  public BinaryComparator(Class<? extends WritableComparable> keyClass) {
    super(keyClass);
  }

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
    this.leftOffset = conf.getInt(LEFT_OFFSET_PROPERTY_NAME, 0);
    this.rightOffset = conf.getInt(RIGHT_OFFSET_PROPERTY_NAME, -1);
    this.keyClass = conf.getClass(MRJobConfig.MAP_OUTPUT_KEY_CLASS, BytesWritable.class, BinaryComparable.class);
  }

  public Configuration getConf() {
    return conf;
  }

  public int compare(WritableComparable w1, WritableComparable w2) {
    BinaryComparable bc1 = (BinaryComparable) w1;
    BinaryComparable bc2 = (BinaryComparable) w2;
    return compareRawBytes(bc1.getBytes(), 0, bc1.getLength(),
                           bc2.getBytes(), 0, bc2.getLength());
  }

  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int n1 = 0;
    int n2 = 0;
    if (keyClass == BytesWritable.class) {
      n1 = 4;
      n2 = 4;
    } else if (keyClass == Text.class) {
      n1 = WritableUtils.decodeVIntSize(b1[s1]);
      n2 = WritableUtils.decodeVIntSize(b2[s2]);
    }
    return compareRawBytes(b1, s1 + n1, l1 - n1,
                           b2, s2 + n2, l2 - n2);
  }

  public int compareRawBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int b1LeftIndex = (leftOffset + s1 + l1) % l1;
    int b1RightIndex = (rightOffset + s1 + l1) % l1;
    int b2LeftIndex = (leftOffset + s2 + l2) % l2;
    int b2RightIndex = (rightOffset + s2 + l2) % l2;
    return WritableComparator.compareBytes(b1, b1LeftIndex, b1RightIndex - b1LeftIndex + 1,
                                           b2, b2LeftIndex, b2RightIndex - b2LeftIndex + 1);
  }
}
