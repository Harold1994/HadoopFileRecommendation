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
import java.util.*;

public class Step4 {
    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();
        private final static Map<Integer,List<Cooccurrence>> cooccurrenceMatrix = new HashMap<Integer, List<Cooccurrence>>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());

            String[] v1 = tokens[0].split(":");
            String[] v2 = tokens[1].split(":");

            if (v1.length > 1) {// cooccurrence
                int itemID1 = Integer.parseInt(v1[0]);
                int itemID2 = Integer.parseInt(v1[1]);
                int num = Integer.parseInt(tokens[1]);

                List<Cooccurrence> list = null;
                if (!cooccurrenceMatrix.containsKey(itemID1)) {
                    list = new ArrayList<Cooccurrence>();
                } else {
                    list = cooccurrenceMatrix.get(itemID1);
                }
                list.add(new Cooccurrence(itemID1, itemID2, num));
                cooccurrenceMatrix.put(itemID1, list);
            }

            if (v2.length > 1) {// userVector
                int itemID = Integer.parseInt(tokens[0]);
                int userID = Integer.parseInt(v2[0]);
                double pref = Double.parseDouble(v2[1]);
                k.set(userID);
                for (Cooccurrence co : cooccurrenceMatrix.get(itemID)) {
                    v.set(co.getItemID2() + "," + pref * co.getNum());
                    context.write(k,v);
                }
            }
        }
    }

    public static class Step4_AggregateAndRecommendReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> result = new HashMap<String, Double>();
            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                String [] str = iter.next().toString().split(",");
                if (result.containsKey(str[0])) {
                    result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                } else {
                    result.put(str[0], Double.parseDouble(str[1]));
                }
            }
            Iterator<String> iter2 = result.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter2.next();
                double score = result.get(itemID);
                v.set(itemID + "," + score);
                context.write(key,v);
            }
        }
    }
    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        String input1 = "hdfs://localhost/user/hdfs/recommend/step3_1";
        String input2 = "hdfs://localhost/user/hdfs/recommend/step3_2";
        String output = path.get("Step4Output");

        Configuration conf = new Configuration();
//        conf.set("mapreduce.task.io.sort.mb","1024");//任务内部排序缓冲区大小,默认为100

        Job job = new Job(conf, "Step4");
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        job.setJarByClass(Step4.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_AggregateAndRecommendReducer.class);
        job.setCombinerClass(Step4_AggregateAndRecommendReducer.class);

        FileInputFormat.setInputPaths(job,new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
class Cooccurrence {
    private int itemID1;
    private int itemID2;
    private int num;

    public Cooccurrence(int itemID1, int itemID2, int num) {
        super();
        this.itemID1 = itemID1;
        this.itemID2 = itemID2;
        this.num = num;
    }

    public int getItemID1() {
        return itemID1;
    }

    public int getItemID2() {
        return itemID2;
    }

    public int getNum() {
        return num;
    }

    public void setItemID1(int itemID1) {
        this.itemID1 = itemID1;
    }

    public void setItemID2(int itemID2) {
        this.itemID2 = itemID2;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
