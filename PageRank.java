import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class PageRank {
    public static class PageRankMapper extends Mapper<Object, Text, IntWritable, PRNodeWritable> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable(value.toString(), true);
            node.missingMask = node.adjacencyList.size() == 0 ? node.pageRank : 0;
            double pageRank = node.adjacencyList.size() == 0 ? 0 : (node.pageRank / node.adjacencyList.size());
            context.write(new IntWritable(node.nodeID), node);
            // for (int i = 0; i < node.adjacencyList.size(); i++) {
            for (Integer nodeID: node.adjacencyList) {
                node = new PRNodeWritable();
                node.nodeID = nodeID;
                node.pageRank = pageRank;
                context.write(new IntWritable(nodeID), node);
            }
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, Text> {
        protected void reduce(IntWritable nodeID, Iterable<PRNodeWritable> nodes, Context context) throws IOException, InterruptedException {
            PRNodeWritable m = null;
            double s = 0;
            for (PRNodeWritable node: nodes) {
                if (node.isNode()) {
                    m = new PRNodeWritable(node);
                } else {
                    s += node.pageRank;
                }
            }
            m.pageRank = s;
            context.write(new IntWritable(m.nodeID), new Text(m.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int numberOfIterations = Integer.parseInt(args[2]);
        conf.set("random_jump_factor", args[3]);
        conf.set("threshold", args[4]);

        conf.set("mapred.textoutputformat.separator", " ");
        conf.setBoolean("last_iteration", false);
        FileSystem hdfs = FileSystem.get(conf);
        Path countPath = new Path("/pagerank_preprocess_count");
        Job job;

        job = Job.getInstance(conf, "PRPreProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcess.PRPreProcessMapper.class);
        job.setReducerClass(PRPreProcess.PRPreProcessReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, countPath);
        job.waitForCompletion(true);

        inputPath = new Path("/pagerank_preprocess");

        job = Job.getInstance(conf, "PRPreProcessCount");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcess.PRPreProcessCountMapper.class);
        job.setReducerClass(PRPreProcess.PRPreProcessCountReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, countPath);
        FileOutputFormat.setOutputPath(job, inputPath);
        job.waitForCompletion(true);

        if (hdfs.exists(countPath)) {
            hdfs.delete(countPath, true);
        }

        int iteration = 0;

        while (iteration < numberOfIterations) {
            Path path = new Path("/pagerank_" + iteration);

            job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(PRNodeWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, path);
            job.waitForCompletion(true);

            if (iteration == numberOfIterations - 1) {
                conf.setBoolean("last_iteration", true);
                inputPath = outputPath;
            } else {
                inputPath = new Path("/pagerank_" + iteration + "_adjust");   
            }

            job = Job.getInstance(conf, "PRAdjust");
            job.setJarByClass(PRAdjust.class);
            job.setMapperClass(PRAdjust.PRAdjustMapper.class);
            job.setReducerClass(PRAdjust.PRAdjustReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(PRNodeWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, path);
            FileOutputFormat.setOutputPath(job, inputPath);
            job.waitForCompletion(true);

            iteration++;
        }
    }
}
