import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class NgramCount {
    public static class NgramCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Map<Text, Integer> map;
        private List<String> words;
        private int n;

        private boolean isAlphanumeric(char c) {
            return (c >= 48 && c <= 57) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
        }

        private Text getKey() {
            String s = "";
            for (int i = 0; i < n; i++) {
                s += " " + words.get(i);
            }
            if (s.length() > 0) {
                s = s.substring(1);
            }
            return new Text(s);
        }

        private void process(String line, int start, int end) {
            if (start != end) {
                words.add(line.substring(start, end));
                if (words.size() == n) {
                    Text key = getKey();
                    Integer count = map.get(key);
                    map.put(key, count == null ? 1 : count + 1);
                    words.remove(0);
                }
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            map = new HashMap<Text, Integer>();
            words = new ArrayList<String>();
            n = Integer.parseInt(context.getConfiguration().get("N"));
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int start = 0;
            int end;
            for (end = 0; end < line.length(); end++) {
                if (!isAlphanumeric(line.charAt(end))) {
                    process(line, start, end);
                    start = end + 1;
                }
            }
            process(line, start, end);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text key: map.keySet()) {
                context.write(key, new IntWritable(map.get(key)));
            }
        }
    }

    public static class NgramCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable intWritable = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            intWritable.set(sum);
            context.write(key, intWritable);
        }
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 3) {
            System.out.println("DEBUG: Not enough arguments");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        try {
            if (Integer.parseInt(args[2]) > 0) {
                conf.set("N", args[2]);
            } else {
                throw new Exception();
            }
        } catch (Exception e) {
            System.out.println("DEBUG: Invalid N");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "NgramCount");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(NgramCountMapper.class);
        job.setCombinerClass(NgramCountReducer.class);
        job.setReducerClass(NgramCountReducer.class);
        job.setOutputKeyClass(Text.class);
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
