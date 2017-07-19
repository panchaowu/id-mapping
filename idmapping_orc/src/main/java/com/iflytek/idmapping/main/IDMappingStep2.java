package com.iflytek.idmapping.main;

import com.iflytek.idmapping.util.IdMappingUtil;
import com.iflytek.idmapping.util.OrcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
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
/**
 * Created by admin on 2017/5/4.
 */
public class IDMappingStep2 implements Tool {
    private Configuration conf = new Configuration();

    public static class Step2M extends Mapper<NullWritable, Writable, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
        }

        @Override
        protected void map(NullWritable key, Writable value1, Context context) throws IOException, InterruptedException {
            OrcStruct value = (OrcStruct)value1;
            String globalId = OrcUtil.getIDsGlobalId(value);
         //   context.write(new Text(globalId),value1);
            String jsonVal = IdMappingUtil.orc2Json(value);
            context.write(new Text(globalId),new Text(jsonVal));
        }

    }

    public static class Step2R extends Reducer<Text, Text, NullWritable, Writable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
            OrcUtil.initialize(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            OrcStruct sumOrc = OrcUtil.InitIDs();
//            for(Writable ids : values) {
            for(Text ids : values) {
             //   OrcStruct val = (OrcStruct) ids;
                OrcStruct val = IdMappingUtil.json2orc(ids.toString());
                OrcUtil.addIDs(val,sumOrc);
            }
            Writable result = OrcUtil.idsSerde.serialize(sumOrc,OrcUtil.idsInspector);
            context.write(NullWritable.get(),result);
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(conf);
        String input = args[0];
        String output = args[1];
        System.out.println("InputPath:" + input);
        System.out.println("OutputputPath:" + output);
        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //map类型设置
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
       // job.setMapOutputValueClass(Writable.class);
        job.setMapOutputValueClass(Text.class);
        //reduce类型设置
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        //job
        job.setJarByClass(IDMappingStep2.class);
        job.setMapperClass(Step2M.class);
        job.setReducerClass(Step2R.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new IDMappingStep2(), args);
        System.exit(res);
    }
}
