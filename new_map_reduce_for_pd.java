    public static class PDResultMapper extends Mapper<Object, Text, IntWritable, Text> { 
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PDNodeWritable node = new PDNodeWritable(value.toString());
            context.write(new IntWritable(node.nodeID), new Text(node.toString()));

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static class PDResultReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
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
                context.write(source, text);
                if (isNode(text)) {
                    m = new PDNodeWritable(source.toString() + " " + text.toString());
                }
            }
            // if (m != null) {
            //     if(m.distance != min) {
            //         context.write(new IntWritable(m.nodeID), new Text(m.toResultString()));
            //     }
            // }        
        }
    }