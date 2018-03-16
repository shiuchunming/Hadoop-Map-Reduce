import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PRPreProcess {
    public static class PRPreProcessMapper extends Mapper<Object, Text, IntWritable, Text> {
        private int index = 0;
        private int source;
        private int destination;
        private Map<IntWritable, Integer> map;

        private void process(String line, int start, int end, Context context) throws IOException, InterruptedException {
            int value = Integer.parseInt(line.substring(start, end));
            if (index == 0) {
                source = value;
            } else if (index == 1) {
                destination = value;
                IntWritable s = new IntWritable(source);
                IntWritable d = new IntWritable(destination);
                // if (source != destination) {
                context.write(s, new Text("" + destination));
                // }
                // map.put(s, 1);
                map.put(d, 1);
            }
            index += 1;
            if (index == 3) {
                index = 0;
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            map = new HashMap<IntWritable, Integer>();
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
                // context.write(source, source);
                context.write(source, new Text("A"));
            }
        }
    }

    public static class PRPreProcessReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable source, Iterable<Text> destinations, Context context) throws IOException, InterruptedException {
            // Map<IntWritable, Integer> map = new HashMap<IntWritable, Integer>();
            String s = "";
            for (Text destination: destinations) {
                String dummy = destination.toString();
                if (!dummy.equals("A")) {
                    s += " " + dummy.toString();
                }
                // if (source.equals(destination)) {
                //     continue;
                // } else if (map.get(destination) == null) {
                //     s += " " + destination.toString();
                //     map.put(destination, 1);
                // }
            }
            if (s.length() > 0) {
                s = s.substring(1);
            }
            context.write(source, new Text(s));
        }
    }

    public static class PRPreProcessCountMapper extends Mapper<Object, Text, IntWritable, Text> {
        private static IntWritable one = new IntWritable(1);
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one, value);
        }
    }

    public static class PRPreProcessCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {          
            List<PRNodeWritable> list = new ArrayList<PRNodeWritable>();
            int count = 0;
            for (Text text : values) {
                count++;
                list.add(new PRNodeWritable(text.toString(), false));
            }
            for (PRNodeWritable node: list) {
                node.numberOfNodes = count;
                node.pageRank = 1.0 / count;
                context.write(new IntWritable(node.nodeID), new Text(node.toString()));
            }
        }
    }
}
