package com.iflytek.idmapping.Index;

import com.iflytek.idmapping.util.OrcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

/**
 * Created by admin on 2017/5/26.
 */
public class GenUniqueIndex implements Tool {

    public static final Log log = LogFactory.getLog(GenUniqueIndex.class);
    private Configuration conf = new Configuration();


    public static class IndexMapper extends Mapper<NullWritable, Writable, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
        }
        @Override
        protected void map(NullWritable key, Writable value, Context context) throws IOException, InterruptedException {
            OrcStruct orcs = (OrcStruct) value;
            String globalId = OrcUtil.getIDsGlobalId(orcs);
            for(int i = 1; i <= OrcUtil.typeCount; i++) {
                Map<Text,IntWritable> typeMap = OrcUtil.getIDsTypeMap(orcs,i);
                //多路输出路径只能是英文和数字
                String idsType = OrcUtil.IdsTypes[i].replace("_","");
                for(Text id : typeMap.keySet()) {
                    context.write(id,new Text(idsType + "##" + globalId));
                }
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, NullWritable, Writable> {
        private MultipleOutputs<NullWritable,Writable> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<NullWritable, Writable>(context);
            IndexOrcUtil.setupIndexOrc();
            IndexOrcUtil.initializeIndex(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val : values) {
                String productAndGlobalId = val.toString();
                String[] splits = productAndGlobalId.split("##");
                if(splits.length == 2) {
                    OrcStruct index = IndexOrcUtil.initIndex();
                    IndexOrcUtil.setIndexId(index,key.toString());
                    IndexOrcUtil.setIndexGlobalId(index,splits[1]);
                    mos.write(splits[0],NullWritable.get(),
                            IndexOrcUtil.indexSerde.serialize(index,IndexOrcUtil.indexInspector),splits[0] + "/");
                    break;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //super.cleanup(context);
            mos.close();
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
        System.out.println("OutputPath:" + output);
        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //map类型设置
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        for(int i = 1; i <= OrcUtil.typeCount; i++) {
            String product = OrcUtil.IdsTypes[i].replace("_","");
            MultipleOutputs.addNamedOutput(job,product,OrcNewOutputFormat.class,NullWritable.class,Writable.class);
        }
        //reduce类型设置
//        job.setOutputFormatClass(OrcNewOutputFormat.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Writable.class);
        job.setJarByClass(GenUniqueIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new GenUniqueIndex(), args);
        System.exit(res);
    }

}
