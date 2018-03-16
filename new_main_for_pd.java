  public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        boolean isDone = false;
        String inputPath = args[0];
        String outputPath = args[1];
        String tmpOutputPathTemplate = "/pd_tmp_output";
        String tmpOutputPath = "/pd_tmp_output";
        int numOfInteration = Integer.parseInt(args[2]);
        int counter = 0;

        while(isDone == false && counter < numOfInteration){
            if(counter == 0){
                inputPath = inputPath;
            }else{
                inputPath = tmpOutputPath;
            }
            tmpOutputPath = tmpOutputPathTemplate+"_"+counter;
            Job job = Job.getInstance(conf, "PDPreProcess");
            job.setJarByClass(PD.class);
            job.setMapperClass(PDMapper.class);
            // job.setCombinerClass(PDReducer.class);
            job.setReducerClass(PDReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(tmpOutputPath));
            // System.exit(job.waitForCompletion(true) ? 0 : 1);
            job.waitForCompletion(true);
            counter++;
        }

        Job job = Job.getInstance(conf, "PDPreProcess");
        job.setJarByClass(PD.class);
        job.setMapperClass(PDResultMapper.class);
        job.setReducerClass(PDResultReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(tmpOutputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);


    }