package flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable , Text ,Text ,FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        String phone = words[1];
        long upFlow = Long.parseLong(words[words.length-3]);
        long downFlow = Long.valueOf(words[words.length-2]);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpFlow(upFlow);
        flowBean.setSumFlow(upFlow , downFlow);
        context.write(new Text(phone),flowBean);
    }
}
