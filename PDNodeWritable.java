import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

public class PDNodeWritable implements Writable {
    public int nodeID;
    public int distance;
    public List<Integer[]> adjacencyList;
    private int index;
    private int temp;
    private boolean flag;
    private boolean withParams;

    private void process(String line, int start, int end) {
        int value = Integer.parseInt(line.substring(start, end));
        if (index == 0) {
            nodeID = value;
        } else {
            if (withParams) {
                if (index == 1) {
                    distance = value;
                } else if (index % 2 == 0) {
                    temp = value;
                } else {
                    Integer node[] = new Integer[2];
                    node[0] = temp;
                    node[1] = value;
                    adjacencyList.add(node);
                }
            } else {
                if (index == 1) {
                    temp = value;
                } else {
                    Integer node[] = new Integer[2];
                    node[0] = temp;
                    node[1] = value;
                    adjacencyList.add(node);   
                }
            }
        }
        index++;
    }

    public PDNodeWritable() {
        adjacencyList = new ArrayList<Integer[]>();
        flag = false;
    }

    public PDNodeWritable(String line, boolean withParams) {
        this();
        this.withParams = withParams;
        index = 0;
        int start = 0;
        int end;
        for (end = 0; end < line.length(); end++) {
            if (line.charAt(end) == ' ') {
                process(line, start, end);
                start = end + 1;
            }
        }
        if (start != end) {
            process(line, start, end);
        }
        flag = true;
    }

    public PDNodeWritable(PDNodeWritable node) {
        nodeID = node.nodeID;
        distance = node.distance;
        adjacencyList = node.adjacencyList;
        flag = true;
    }

    public boolean isNode() {
        return flag;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeID);
        out.writeInt(distance);
        out.writeInt(adjacencyList.size());
        for (Integer[] node: adjacencyList) {
            out.writeInt(node[0]);
            out.writeInt(node[1]);
        }
        out.writeBoolean(flag);
    }
   
    public void readFields(DataInput in) throws IOException {
        nodeID = in.readInt();
        distance = in.readInt();
        int size = in.readInt();
        adjacencyList = new ArrayList<Integer[]>();
        for (int i = 0; i < size; i++) {
            Integer node[] = new Integer[2];
            node[0] = in.readInt();
            node[1] = in.readInt();
            adjacencyList.add(node);
        }
        flag = in.readBoolean();
    }

    public String toString() {
        String s = "" + distance;
        for (Integer[] node: adjacencyList) {
            s += " " + node[0] + " " + node[1];
        }
        return s;
    }
}