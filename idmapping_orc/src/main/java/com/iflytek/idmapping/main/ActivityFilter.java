package com.iflytek.idmapping.main;

import com.iflytek.idmapping.util.OrcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
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
 * Created by admin on 2017/7/14.
 */
public class ActivityFilter implements Tool {
    public static final Log log = LogFactory.getLog(ActivityFilter.class);

    private Configuration conf = new Configuration();

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class FilterM extends Mapper<NullWritable, Writable, NullWritable, Writable> {

        public int minDay = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
            OrcUtil.initialize(context);
            System.out.println("filterday:" + context.getConfiguration().get("filterday"));
            minDay = Integer.parseInt(context.getConfiguration().get("filterday"));
        }

        @Override
        protected void map(NullWritable key, Writable value, Context context) throws IOException, InterruptedException {
            OrcStruct newIDs = OrcUtil.filterIDs((OrcStruct)value,minDay);
            if(newIDs != null) {
                context.write(NullWritable.get(), OrcUtil.idsSerde.serialize(newIDs,OrcUtil.idsInspector));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];
        String filterday = args[2];
        System.out.println("Input:" + input);
        System.out.println("Output:" + output);
        System.out.println("filterday:" + filterday);

        conf.set("filterday",filterday);
        Job job = new Job(conf);
        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //map类型设置
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        job.setNumReduceTasks(0);
        job.setJarByClass(ActivityFilter.class);
        job.setMapperClass(FilterM.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ActivityFilter(), args);
        System.exit(res);
    }

}
