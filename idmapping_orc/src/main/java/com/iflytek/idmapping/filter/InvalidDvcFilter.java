package com.iflytek.idmapping.filter;

import com.iflytek.avro.mapreduce.output.TextOutputFormat;
import com.iflytek.idmapping.util.OrcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

/**
 * Created by admin on 2017/6/14.
 */
public class InvalidDvcFilter implements Tool {
    private Configuration conf = new Configuration();

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class FilterDvcMap extends Mapper<NullWritable, Writable, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
        }

        @Override
        protected void map(NullWritable key, Writable value1, Context context) throws IOException, InterruptedException {
            OrcStruct srcOrc = (OrcStruct)value1;
            OrcStruct value = OrcUtil.toLowerCase(srcOrc);
            Map<String,Integer> ids = OrcUtil.mergeIDsToMap(value);
            OrcUtil.setIDsGlobalId(value,"");
            if (ids != null && ids.size() != 0) {
//                String secondKey = (String)ids.keySet().toArray()[0] + ids.keySet().toArray()[ids.keySet().size() - 1]
//                        + IdMappingUtil.getRandomString(context.getTaskAttemptID().toString(), Integer.MAX_VALUE);
//                OrcUtil.setIDsGlobalId(value,secondKey);
//                String IDsStr = IdMappingUtil.orc2Json(value);
                String IDsStr = " ";
                for (Map.Entry<String, Integer> entry : ids.entrySet()) {
                    String idValue = entry.getKey();
                    context.write(new Text(idValue),new Text(IDsStr));
                }
            }
        }
    }

    public static class FilterDvcReduce extends Reducer<Text,Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Text val : values) {
                count++;
                if(count > 10000000) {
                    context.write(key,NullWritable.get());
                    break;
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(conf,"get filter dvc");
        String input = args[0];
        String output = args[1];
        System.out.println("input:" + input);
        System.out.println("output:" + output);

       // conf.set("mapreduce.job.queuename", "ydhl");
        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //map类型设置
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //reduce类型设置
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(InvalidDvcFilter.class);
        job.setMapperClass(FilterDvcMap.class);
        job.setReducerClass(FilterDvcReduce.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new InvalidDvcFilter(), args);
        System.exit(res);
    }
}
