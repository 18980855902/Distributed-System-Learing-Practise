import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

public class totalnumber{
  public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text age = new Text();
    private final static IntWritable count = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split(",");
      String dateString = fields[0];
      String ageString = fields[1];
      int countInt = Integer.parseInt(fields[3]);

      String[] dateFields = dateString.split("-");
      String yearString = dateFields[0];
      String monthString = dateFields[1];

      if ((!yearString.equals("2021") && !monthString.equals("12"))||
              (!yearString.equals("2022") && !monthString.equals("01"))||
              (!yearString.equals("2022") && !monthString.equals("02"))||
              (!yearString.equals("2022") && !monthString.equals("03"))) {
        age.set(ageString);
        count.set(countInt);
        context.write(age, count);
      }else return;
    }
  }
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    job.setJarByClass(totalnumber.class);
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
