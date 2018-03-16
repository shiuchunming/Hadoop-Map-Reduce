import java.io.IOException;
import java.math.BigDecimal;
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

public class PRAdjust {
    public static class PRAdjustMapper extends Mapper<Object, Text, IntWritable, PRNodeWritable> {
        private List<PRNodeWritable> nodes;
        private double missingMask;

        protected void setup(Context context) throws IOException, InterruptedException {
            nodes = new ArrayList<PRNodeWritable>();
            missingMask = 0.0;
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable(value.toString(), true);
            nodes.add(node); 
            missingMask += node.missingMask;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (PRNodeWritable node: nodes) {
                node.missingMask = missingMask;
                context.write(new IntWritable(node.nodeID), node);
            }
        }
    }

    public static class PRAdjustReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, Text> {
        private double randomJumpFactor;
        private double threshold;
        private boolean lastIteration;

        private double adjust(PRNodeWritable node) {
            return randomJumpFactor * (1.0 / node.numberOfNodes) + (1 - randomJumpFactor) * (node.missingMask / node.numberOfNodes + node.pageRank);
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            randomJumpFactor = Double.parseDouble(context.getConfiguration().get("random_jump_factor"));
            threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
            lastIteration = context.getConfiguration().getBoolean("last_iteration", false);
        }

        protected void reduce(IntWritable nodeID, Iterable<PRNodeWritable> nodes, Context context) throws IOException, InterruptedException {
            for (PRNodeWritable node: nodes) {
                node.pageRank = adjust(node);
                node.missingMask = 0;
                if (lastIteration) {
                    if (node.pageRank > threshold) {
                        // String.format("%.8f", node.pageRank)
                        context.write(new IntWritable(node.nodeID), new Text("" + node.pageRank));
                    }
                } else {
                    context.write(new IntWritable(node.nodeID), new Text(node.toString()));
                }
            }
        }
    }
}
