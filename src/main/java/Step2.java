import hdfs.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class Step2 {
    public static class Step2_UserVectorToconcurrentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] tokens = Recommend.DELIMITER.split(value.toString());
            for(int i = 1; i<tokens.length; i++) {
                String itemId = tokens[i].split(":")[0];
                for(int j = 1; j<tokens.length; j++) {
                    String itemId2 = tokens[j].split(":")[0];
                    context.write(new Text(itemId + ":" + itemId2), v);
                }
            }
        }
    }

    public static class Step2_UserVectorToconcurrentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator <IntWritable> iter = values.iterator();
            while(iter.hasNext()) {
                sum += iter.next().get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.task.io.sort.mb","1024");//任务内部排序缓冲区大小,默认为100
        Job job = new Job(conf, "Step2");

        String input = path.get("Step2Input");
        String output = path.get("Step2Output");
        System.out.println(output);
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        job.setJarByClass(Step2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Step2_UserVectorToconcurrentMapper.class);
        job.setCombinerClass(Step2_UserVectorToconcurrentReducer.class);
        job.setReducerClass(Step2_UserVectorToconcurrentReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

    }
}
