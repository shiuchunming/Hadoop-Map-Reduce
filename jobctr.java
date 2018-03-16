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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PD {
    public static class PDMapper extends Mapper<Object, Text, IntWritable, Text> { 
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PDNodeWritable node = new PDNodeWritable(value.toString());
            context.write(new IntWritable(node.nodeID), new Text(node.toString()));
            for (Integer[] neighbor : node.adjacencyList) {
                int distance;
                if (node.distance != Integer.MAX_VALUE) {
                    distance = node.distance + neighbor[1];
                } else {
                    distance = node.distance;
                }
                context.write(new IntWritable(neighbor[0]), new Text("" + distance));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static class PDReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private boolean isNode(Text text) {
            boolean b = false;
            String s = text.toString();
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == ' ') {
                    b = true;
                    break;
                }
            }
            return b;
        }

        protected void reduce(IntWritable source, Iterable<Text> nodes, Context context) throws IOException, InterruptedException {
            int min = Integer.MAX_VALUE;
            int state = 0;
            PDNodeWritable m = null;
            for (Text text : nodes) {
                if (isNode(text)) {
                    m = new PDNodeWritable(source.toString() + " " + text.toString());
                } else {
                    int distance = Integer.parseInt(text.toString());
                    if (distance < min) {
                        min = distance;
                        state = 1;
                    }
                }   
            }
            if (m != null) {
                if(m.distance < min) {
                }else{
                    m.distance = min;
                }
                m.state = state;
                context.write(new IntWritable(m.nodeID), new Text(m.toString()));
            }else{
                context.write(source, new Text(min + " " + state));
            }        
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuration conf = new Configuration();
        JobConf conf = new JobConf(JobCtrlTest.class);
        conf.set("mapred.textoutputformat.separator", " ");
        // conf.set("source_node", args[3]);
        boolean isDone = false;
        String inputPath = args[0];
        String outputPath = args[1];
        
        JobControl jc = new JobControl();
        Job job[] = new Job[5];
        int counter = 0;
        JobControl jobCtrl = new JobControl("myctrl");

        while(isDone == false && counter < 5){
            job[counter] = Job.getInstance(conf, "PDPreProcess");
            job[counter].setJarByClass(PD.class);
            job[counter].setMapperClass(PDMapper.class);
            // job.setCombinerClass(PDReducer.class);
            job[counter].setReducerClass(PDReducer.class);
            job[counter].setOutputKeyClass(IntWritable.class);
            job[counter].setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job[counter], new Path(inputPath));
            FileOutputFormat.setOutputPath(job[counter], new Path(outputPath));
            // System.exit(job.waitForCompletion(true) ? 0 : 1);
            // job.waitForCompletion(true);
            ctrljob[counter] = new ControlledJob(conf);
            ctrljob[counter].setJob(job[counter]);
            if(counter != 0){
                ctrljob[counter].addDependingJob(ctrljob[counter-1]);
            }
            jobCtrl.addJob(ctrljob[counter]);

            counter++;
            inputPath = outputPath;
            outputPath = args[1]+"_"+counter;
        }


        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {

            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }

    }
}
