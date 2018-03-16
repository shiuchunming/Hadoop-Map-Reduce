import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParallelDijkstra {
    public static enum ReachCounter { COUNT };

    public static class ParallelDijkstraMapper extends Mapper<Object, Text, IntWritable, PDNodeWritable> { 
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PDNodeWritable node = new PDNodeWritable(value.toString(), true);
            int distance = node.distance;
            context.write(new IntWritable(node.nodeID), node);
            for (Integer[] neighbor : node.adjacencyList) {
                PDNodeWritable outgoingDistanceNode = new PDNodeWritable();
                outgoingDistanceNode.nodeID = neighbor[0];
                outgoingDistanceNode.distance = distance;
                if (outgoingDistanceNode.distance != Integer.MAX_VALUE) {
                    outgoingDistanceNode.distance += neighbor[1];
                }
                context.write(new IntWritable(outgoingDistanceNode.nodeID), outgoingDistanceNode);
            }
        }
    }

    public static class ParallelDijkstraReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {
        protected void reduce(IntWritable source, Iterable<PDNodeWritable> nodes, Context context) throws IOException, InterruptedException {
            int min = Integer.MAX_VALUE;
            PDNodeWritable m = null;
            for (PDNodeWritable node: nodes) {
                if (node.isNode()) {
                    m = new PDNodeWritable(node);
                } else if (node.distance < min) {
                    min = node.distance;
                }   
            }
            if (m.distance > min) {
                m.distance = min;
                context.getCounter(ReachCounter.COUNT).increment(1);
            }
            context.write(source, new Text(m.toString()));
        }
    }

    public static class ParallelDijkstraResultMapper extends Mapper<Object, Text, IntWritable, PDNodeWritable> { 
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PDNodeWritable node = new PDNodeWritable(value.toString(), true);
            context.write(new IntWritable(node.nodeID), node);
        }
    }

    public static class ParallelDijkstraResultReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, IntWritable> {
        protected void reduce(IntWritable source, Iterable<PDNodeWritable> nodes, Context context) throws IOException, InterruptedException {
            for (PDNodeWritable node: nodes) {
                if (node.distance != Integer.MAX_VALUE) {
                    context.write(new IntWritable(node.nodeID), new IntWritable(node.distance));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        FileSystem hdfs = FileSystem.get(conf);
        //
        conf.set("source", args[2]);
        String inputPath = args[0];
        String outputPath = args[1];
        int numOfIteration = Integer.parseInt(args[3]);
        String pdInputPath = "/pd_tmp_output_preprocessed";
        String tmpOutputPathTemplate = "/pd_tmp_output";
        String tmpOutputPath = "/pd_tmp_output";
        int counter = 0;
        // Preprocessing
        Job job = Job.getInstance(conf, "PDPreProcess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PDPreProcess.PDPreProcessMapper.class);
        // job.setCombinerClass(PDPreProcessReducer.class);
        job.setReducerClass(PDPreProcess.PDPreProcessReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PDNodeWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(pdInputPath));
        job.waitForCompletion(true);
        // ParalleDijkstra
        while(numOfIteration == 0 || counter < numOfIteration){
            tmpOutputPath = tmpOutputPathTemplate+"_"+counter;
            job = Job.getInstance(conf, "ParallelDijkstra");
            job.setJarByClass(ParallelDijkstra.class);
            job.setMapperClass(ParallelDijkstraMapper.class);
            // job.setCombinerClass(PDReducer.class);
            job.setReducerClass(ParallelDijkstraReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(PDNodeWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            Path path = new Path(pdInputPath);
            FileInputFormat.addInputPath(job, path);
            FileOutputFormat.setOutputPath(job, new Path(tmpOutputPath));
            job.waitForCompletion(true);
            // if (hdfs.exists(path)) {
            //     hdfs.delete(path, true);
            // }
            long reachCount = job.getCounters().findCounter(ParallelDijkstra.ReachCounter.COUNT).getValue();
            if (reachCount == 0) {
                break;
            }
            pdInputPath = tmpOutputPath;
            counter++;
        }
        job = Job.getInstance(conf, "ParallelDijkstraResult");
        job.setJarByClass(ParallelDijkstra.class);
        job.setMapperClass(ParallelDijkstraResultMapper.class);
        job.setReducerClass(ParallelDijkstraResultReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PDNodeWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        Path path = new Path(tmpOutputPath);
        FileInputFormat.addInputPath(job, path);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
        if (hdfs.exists(path)) {
            hdfs.delete(path, true);
        }
    }
}
