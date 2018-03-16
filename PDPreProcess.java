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

public class PDPreProcess {
    public static class PDPreProcessMapper extends Mapper<Object, Text, IntWritable, PDNodeWritable> {
        private int index = 0;
        private int source;
        private int destination;
        private int distance;
        private Map<IntWritable, Integer> map;
        private Map<String, Integer> map2;

        private void process(String line, int start, int end, Context context) throws IOException, InterruptedException {
            int value = Integer.parseInt(line.substring(start, end));
            if (index == 0) {
                source = value;
            } else if (index == 1) {
                destination = value;
            } else if (index == 2) {
                distance = value;
                IntWritable s = new IntWritable(source);
                IntWritable d = new IntWritable(destination);
                if (source != destination) {
                    String key = source + " " + destination;
                    Integer i = map2.get(key);
                    if (i == null || distance < i) {
                        PDNodeWritable node = new PDNodeWritable();
                        node.nodeID = destination;
                        node.distance = distance;
                        context.write(s, node);
                        map2.put(key, distance);
                    }
                }
                map.put(s, 1);
                map.put(d, 1);
            }
            index += 1;
            if (index == 3) {
                index = 0;
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            map = new HashMap<IntWritable, Integer>();
            map2 = new HashMap<String, Integer>();        
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int start = 0;
            int end;
            for (end = 0; end < line.length(); end++) {
                if (line.charAt(end) == ' ') {
                    process(line, start, end, context);
                    start = end + 1;
                }
            }
            process(line, start, end, context);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (IntWritable source: map.keySet()) {
                PDNodeWritable node = new PDNodeWritable();
                node.nodeID = source.get();
                context.write(source, node);
            }
        }
    }

    public static class PDPreProcessReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {
        private static int source;
        protected void setup(Context context) throws IOException, InterruptedException {
            source = Integer.parseInt(context.getConfiguration().get("source"));
        }
        protected void reduce(IntWritable source, Iterable<PDNodeWritable> nodes, Context context) throws IOException, InterruptedException {   
            Map<IntWritable, Integer> map = new HashMap<IntWritable, Integer>();
            String s = "";
            for (PDNodeWritable node: nodes) {
                IntWritable destination = new IntWritable(node.nodeID);
                if (source.equals(destination)) {
                    continue;
                } else if (map.get(destination) == null) {
                    s += " " + node.nodeID + " " + node.distance;
                    map.put(destination, 1);
                }
            }
            int distance = source.get() == this.source ? 0 : Integer.MAX_VALUE;
            context.write(source, new Text(distance + s));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        int source = Integer.parseInt(args[2]);
        // TODO: check before pass to conf
        conf.set("source", args[2]);
        Job job = Job.getInstance(conf, "PDPreProcess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PDPreProcessMapper.class);
        // job.setCombinerClass(PDPreProcessReducer.class);
        job.setReducerClass(PDPreProcessReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PDNodeWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
