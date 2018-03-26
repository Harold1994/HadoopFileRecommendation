import hdfs.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Step4_Update {


    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;//A同现矩阵,B用户评分矩阵

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
            //System.out.println(flag);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] tokens = Recommend.DELIMITER.split(value.toString());
            if (flag.equals("step3_2")) {
                String [] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "," + num);
                context.write(k,v);
            }

            else if (flag.equals("step3_1")) {
                String [] v2 = tokens[1].split(":");
                String itemID = tokens[0];
                String userID = v2[0];
                String pref = v2[1];
                Text k = new Text(itemID);
                Text v = new Text("B:" + userID + "," + pref);
                context.write(k,v);
            }
        }
    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println(key.toString() + ":");
            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();
            for (Text line : values) {
                String val = line.toString();
                //System.out.println(val);

                if (val.startsWith("A:")) {
                    String [] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);
                } else if (val.startsWith("B:")) {
                    String [] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);
                }
            }

            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();//itemID2
                int num = Integer.parseInt(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();//userID
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();

                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;
                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "," + result);
                    context.write(k, v);
                    //System.out.println(k.toString() + "  " + v.toString());
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        String input1 = path.get("Step5Input1");
        String input2 = path.get("Step5Input2");
        String output = path.get("Step5Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        job.setJarByClass(Step4_Update.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_Update.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_Update.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
