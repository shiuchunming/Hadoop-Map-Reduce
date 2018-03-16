import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF {
    public static class NgramRFMapper extends Mapper<Object, Text, Text, MapWritable> {
        private static Text asterisk = new Text("*");        
        private static IntWritable one = new IntWritable(1);
        private List<String> words;
        private int n;

        private boolean isAlphanumeric(char c) {
            return (c >= 48 && c <= 57) || (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
        }

        private Text getKey() {
            String word = words.size() > 0 ? words.get(0) : "";
            return new Text(word);
        }

        private Text getSubKey() {
            String s = "";
            for (int i = 1; i < n; i++) {
                s += " " + words.get(i);
            }
            if (s.length() > 0) {
                s = s.substring(1);
            }
            return new Text(s);
        }

        private void process(String line, int start, int end, Context context) throws IOException, InterruptedException {
            if (start != end) {
                words.add(line.substring(start, end));
                if (words.size() == n) {
                    Text key = getKey();
                    Text subKey = getSubKey();
                    MapWritable mapWritable = new MapWritable();
                    mapWritable.put(asterisk, one);
                    mapWritable.put(subKey, one);
                    context.write(key, mapWritable);
                    words.remove(0);
                }
            }
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            words = new ArrayList<String>();
            n = Integer.parseInt(context.getConfiguration().get("N"));
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int start = 0;
            int end;
            for (end = 0; end < line.length(); end++) {
                if (!isAlphanumeric(line.charAt(end))) {
                    process(line, start, end, context);
                    start = end + 1;
                }
            }
            process(line, start, end, context);
        }
    }

    public static class NgramRFCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        private MapWritable combine(Iterable<MapWritable> mapWritables) {
            MapWritable combinedMapWritable = new MapWritable();
            for (MapWritable mapWritable : mapWritables) {
                for (Writable writable: mapWritable.keySet()) {
                    Text subKey = (Text) writable;
                    IntWritable combinedCount = (IntWritable) combinedMapWritable.get(subKey);
                    IntWritable count = (IntWritable) mapWritable.get(subKey);
                    if (combinedCount != null && count != null) {
                        combinedMapWritable.put(subKey, new IntWritable(combinedCount.get() + count.get()));
                    } else if (combinedCount == null && count != null) {
                        combinedMapWritable.put(subKey, count);
                    }
                }
            }
            return combinedMapWritable;
        }

        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, combine(values));
        }
    }

    public static class NgramRFReducer extends Reducer<Text, MapWritable, Text, Text> {
        private static Text asterisk = new Text("*");
        private float theta;

        private String pretty(float f) {
            String s = "";
            if (f % 1 == 0) {
                s += (int) f;
            } else {
                s += f;
            }
            return s;
        }

        private MapWritable combine(Iterable<MapWritable> mapWritables) {
            MapWritable combinedMapWritable = new MapWritable();
            for (MapWritable mapWritable : mapWritables) {
                for (Writable writable: mapWritable.keySet()) {
                    Text subKey = (Text) writable;
                    IntWritable combinedCount = (IntWritable) combinedMapWritable.get(subKey);
                    IntWritable count = (IntWritable) mapWritable.get(subKey);
                    if (combinedCount != null && count != null) {
                        combinedMapWritable.put(subKey, new IntWritable(combinedCount.get() + count.get()));
                    } else if (combinedCount == null && count != null) {
                        combinedMapWritable.put(subKey, count);
                    }
                }
            }
            return combinedMapWritable;
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            theta = Float.parseFloat(context.getConfiguration().get("theta"));
        }

        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable mapWritable = combine(values);
            IntWritable asteriskCount = (IntWritable) mapWritable.get(asterisk);
            for (Writable writable: mapWritable.keySet()) {
                Text subKey = (Text) writable;
                if (subKey.equals(asterisk)) {
                    continue;
                }
                IntWritable count = (IntWritable) mapWritable.get(subKey);
                String compositeKey = key.toString() + " " + subKey.toString();
                float relativeFrequency = count.get() * 1.0f / asteriskCount.get();
                if (relativeFrequency >= theta) {
                    context.write(new Text(compositeKey), new Text(pretty(relativeFrequency)));
                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 4) {
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
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("DEBUG: Invalid N");
            System.exit(1);
        }
        try {
            conf.set("theta", args[3]);
        } catch(ArrayIndexOutOfBoundsException e) {
            System.out.println("DEBUG: Invalid theta" + e);
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "NgramRF");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(NgramRFMapper.class);
        job.setCombinerClass(NgramRFCombiner.class);
        job.setReducerClass(NgramRFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
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
