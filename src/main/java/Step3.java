import hdfs.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class Step3 {
    public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable k = new IntWritable();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            for (int i =1; i<tokens.length; i++) {
                String [] vector = tokens[i].split(":");
                int itemId = Integer.parseInt(vector[0]);
                String pref = vector[1];
                k.set(itemId);
                v.set(tokens[0] + ":" + pref);
                context.write(k,v);
            }
        }
    }
    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.task.io.sort.mb","1024");//任务内部排序缓冲区大小,默认为100
        Job job = new Job(conf, "step31_spliteUserVector");
        String input = path.get("Step3Input1");
        String output = path.get("Step3Output1");

        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.rmr(output);
        job.setJarByClass(Step3.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(Step31_UserVectorSplitterMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }

    public static class Step32_CooccurreceColumWrapperMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = Recommend.DELIMITER.split(value.toString());
            k.set(tokens[0]);
            v.set(Integer.parseInt(tokens[1])); //这里和step2的输出有什么区别????
            context.write(k,v);
        }
    }

    public static void run2(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.task.io.sort.mb","1024");//任务内部排序缓冲区大小,默认为100
        Job job = new Job(conf, "step32_cooccurreceMap");
        String input = path.get("Step3Input2");
        String output = path.get("Step3Output2");
        job.setJarByClass(Step3.class);
        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.rmr(output);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(Step32_CooccurreceColumWrapperMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
