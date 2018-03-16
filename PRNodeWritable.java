import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

public class PRNodeWritable implements Writable {
    public int nodeID;
    public int numberOfNodes;
    public double pageRank;
    public double missingMask;
    public List<Integer> adjacencyList;
    private boolean flag;
    private boolean withParams;
    private int index;

    private void process(String line, int start, int end) {
        double value = Double.parseDouble(line.substring(start, end));
        if (index == 0) {
            nodeID = (int)value;
        } else {
            if (withParams) {
                if (index == 1) {
                    numberOfNodes = (int)value;
                } else if (index == 2) {
                    pageRank = value;
                } else if (index == 3) {
                    missingMask = value;
                } else {
                    adjacencyList.add((int)value);
                }
            } else {
                adjacencyList.add((int)value);
            }
        }
        index++;
    }

    public PRNodeWritable() {
        adjacencyList = new ArrayList<Integer>();
        flag = false;
    }

    public PRNodeWritable(String line, boolean withParams) {
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

    public PRNodeWritable(PRNodeWritable node) {
        nodeID = node.nodeID;
        numberOfNodes = node.numberOfNodes;
        pageRank = node.pageRank;
        missingMask = node.missingMask;
        adjacencyList = node.adjacencyList;
        flag = true;
    }

    public boolean isNode() {
        return flag;
    }

    public String toString() {
        String s = numberOfNodes + " " + pageRank + " " + missingMask;
        for (Integer nodeID : adjacencyList) {
            s += " " + nodeID;
        }
        return s;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeID);
        out.writeInt(numberOfNodes);
        out.writeDouble(pageRank);
        out.writeDouble(missingMask);
        out.writeInt(adjacencyList.size());
        for(int nodeID: adjacencyList) {
            out.writeInt(nodeID);
        }
        out.writeBoolean(flag);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID = in.readInt();
        numberOfNodes = in.readInt();
        pageRank = in.readDouble();
        missingMask = in.readDouble();
        int size = in.readInt();
        adjacencyList = new ArrayList<Integer>();
        for (int i = 0; i < size; i++) {
            adjacencyList.add(in.readInt());
        }
        flag = in.readBoolean();
    }
}