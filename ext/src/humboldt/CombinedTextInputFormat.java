package humboldt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

@SuppressWarnings("deprecation")
public class CombinedTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader((CombineFileSplit) split, context, (Class) TextCombinedFileRecordReader.class);
    }

    public static class TextCombinedFileRecordReader extends LineRecordReader {
        private FileSplit filesplit;

        public TextCombinedFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
            super();
            filesplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
        }

        @Override
        public void initialize(InputSplit split,
                               TaskAttemptContext context) throws IOException {
            super.initialize(filesplit, context);
        }
    }
}
