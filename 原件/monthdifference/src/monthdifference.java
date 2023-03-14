import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class monthdifference {
  public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text yearmonth = new Text();
    private final Text nextyearmonth = new Text();
    private final static IntWritable count = new IntWritable();
    private final static IntWritable nextcount = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split(",");
      String dateString = fields[0];
      int countInt1 = Integer.parseInt(fields[9]);
      int countInt2 = Integer.parseInt(fields[9]);

      String[] dateFields = dateString.split("-");
      String yearString = dateFields[0];
      String monthString = dateFields[1];
      int nextYear = Integer.parseInt(yearString);
      int nextMonth = Integer.parseInt(monthString) + 1;
      if (nextMonth > 12) {
        nextMonth = 1;
        nextYear++;
      }
      String nextyearString = Integer.toString(nextYear);
      String nextmonthString = String.format("%02d", nextMonth);

      int countInt = countInt1 + countInt2;
      count.set(countInt);
      nextcount.set(countInt * -1);
      yearmonth.set("Last month and "+yearString + "-" + monthString + "difference");
      nextyearmonth.set("Last month and " + nextyearString + "-" + nextmonthString + "difference");
      context.write(yearmonth, count);
      context.write(nextyearmonth, nextcount);
    }
  }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
      }
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length < 2) {
        System.err.println("Usage: wordcount4 <in> [<in>...] <out>");
        System.exit(2);
      }
      Job job = Job.getInstance(conf, "word count 4");
      job.setJarByClass(monthdifference.class);
      job.setMapperClass(AgeMapper.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      for (int i = 0; i < otherArgs.length - 1; ++i) {
        FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
      }
      FileOutputFormat.setOutputPath(job,
              new Path(otherArgs[otherArgs.length - 1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
