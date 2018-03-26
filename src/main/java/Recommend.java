
import org.apache.hadoop.conf.Configured;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommend extends Configured{
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    public static final String HDFS = "hdfs://localhost/";
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "resources/small.csv");
        path.put("Step1Input", HDFS + "/user/hdfs/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");

        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        path.put("Step5Input1", path.get("Step3Output1"));
        path.put("Step5Input2", path.get("Step3Output2"));
        path.put("Step5Output", path.get("Step1Input") + "/step5");

        path.put("Step6Input", path.get("Step5Output"));
        path.put("Step6Output", path.get("Step1Input") + "/step6");

        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        Step3.run2(path);
//        Step4.run(path);//NullPointerException,可能不会先构造同现矩阵
        Step4_Update.run(path);
        Step4_Update2.run(path);

        System.exit(0);
    }
}
