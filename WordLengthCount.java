import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {
    public static class WordLengthCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable key = new IntWritable();
        private IntWritable value = new IntWritable();
        private Map<Integer, Integer> map;

        private void process(int start, int end) {
            if (start != end) {
                Integer length = end - start;
                map.put(length, map.get(length) == null ? 1 : map.get(length) + 1);  
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            map = new HashMap<Integer, Integer>();
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int start = 0;
            int end;
            for (end = 0; end < line.length(); end++) {
                char c = line.charAt(end);
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                    process(start, end);
                    start = end + 1;
                }
            }
            process(start, end);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer length: map.keySet()) {
                key.set(length);
                value.set(map.get(length));
                context.write(key, value);
            }
        }
    }

    public static class WordLengthCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable value = new IntWritable();

        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable intWritable : values) {
                sum += intWritable.get();
            }
            value.set(sum);
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("DEBUG: Not enough arguments");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "WordLengthCount");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(WordLengthCountMapper.class);
        job.setCombinerClass(WordLengthCountReducer.class);
        job.setReducerClass(WordLengthCountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        } catch (Exception e) {
            System.out.println("DEBUG: Invalid input directory");
            System.exit(1);
        }
        try {
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } catch (Exception e) {
            System.out.println("DEBUG: Invalid output directory");
            System.exit(1);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
