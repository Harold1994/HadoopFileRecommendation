import hdfs.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Step1 {

    public static class Step1_ToItemPreMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens []= Recommend.DELIMITER.split(value.toString());
            int userId = Integer.parseInt(tokens[0]);
            String itemId = tokens[1];
            String pref = tokens[2];
            context.write(new IntWritable(userId), new Text(itemId + ":" + pref));
        }
    }

    public static class Step1_ToUserVectorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                sb.append("," +  iter.next());
            }
            context.write(key, new Text(sb.toString().replaceFirst(",","")));
        }
    }



    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        Configuration conf = new Configuration();
//        conf.set("mapreduce.task.io.sort.mb","1024");//任务内部排序缓冲区大小,默认为100

        Job job = new Job(conf, "Step1");
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"), input);

        job.setJarByClass(Step1.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Step1_ToItemPreMapper.class);
        job.setReducerClass(Step1_ToUserVectorReducer.class);
        job.setCombinerClass(Step1_ToUserVectorReducer.class);

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
