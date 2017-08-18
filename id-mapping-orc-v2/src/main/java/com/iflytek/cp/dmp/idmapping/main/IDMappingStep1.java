//package com.iflytek.cp.dmp.idmapping.main;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.orc.mapred.OrcKey;
//import org.apache.orc.mapred.OrcMap;
//import org.apache.orc.mapred.OrcStruct;
//import org.apache.orc.mapred.OrcValue;
//import org.apache.orc.mapreduce.OrcInputFormat;
//
//import java.io.IOException;
//
///**
// * Created by admin on 2017/7/19.
// */
//public class IDMappingStep1  implements Tool {
//    Configuration conf = new Configuration();
//
//    public static class Step1M extends Mapper<NullWritable, OrcStruct, OrcKey, OrcValue> {
////        struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>
//        @Override
//        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
//            OrcKey orcKey = new OrcKey();
//            OrcValue orcValue = new OrcValue();
//            orcKey.key = value;
//            orcValue.value = value;
//            context.write(orcKey, orcValue);
//        }
//    }
//
//    public static class Step1R extends Reducer<OrcKey, OrcValue, Text, Text> {
//        @Override
//        protected void reduce(OrcKey key, Iterable<OrcValue> values, Context context) throws IOException, InterruptedException {
//            for (OrcValue orcValue: values) {
//                Text outKey = (Text) ((OrcStruct)(orcValue.value)).getFieldValue("global_id");
//                OrcMap<Text, OrcMap> ids = (OrcMap<Text, OrcMap>) ((OrcStruct)(orcValue.value)).getFieldValue("ids");
//                OrcMap imeis = ids.get(new Text("imei"));
//                String outValue = new String();
//                for(Object tmpKey: imeis.keySet()) {
//                    OrcStruct imeiStruct = (OrcStruct) imeis.get(tmpKey);
//                    Text src = (Text) imeiStruct.getFieldValue("src");
//                    IntWritable datetime = (IntWritable) imeiStruct.getFieldValue("datetime");
//                    Text model = (Text) imeiStruct.getFieldValue("model");
//                    outValue = src + "_" + Integer.toString(datetime.get()) + "_" + model + ":" + outValue;
//                }
//                context.write(new Text(outKey), new Text(outValue));
//            }
//        }
//    }
//
//    @Override
//    public int run(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.set("mapreduce.job.queuename", "dmp");
//        conf.set("mapreduce.job.name", "idmappingStep1");
//        conf.set("orc.mapred.map.output.key.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
//        conf.set("orc.mapred.map.output.value.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
//        conf.set("orc.mapred.output.schema", "struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
//        Job job = new Job(conf);
//        String input = args[0];
//        String output = args[1];
//        System.out.println("input: " + input);
//        System.out.println("output: " + output);
//        FileSystem fs = FileSystem.get(conf);
//        Path outputPath = new Path(output);
//        if (fs.exists(outputPath)) {
//            fs.delete(outputPath, true);//如果输出路径存在，就将其删除
//        }
//        // 输入输出路径
//        FileInputFormat.addInputPath(job, new Path(input));
//        FileOutputFormat.setOutputPath(job, new Path(output));
//
//        job.setJarByClass(IDMappingStep1.class);
//        job.setInputFormatClass(OrcInputFormat.class);
//        job.setMapOutputKeyClass(OrcKey.class);
//        job.setMapOutputValueClass(OrcValue.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        job.setJarByClass(IDMappingStep1.class);
//        job.setMapperClass(Step1M.class);
//        job.setReducerClass(Step1R.class);
//        job.setNumReduceTasks(1);
//
//        job.waitForCompletion(true);
//        return 0;
//    }
//
//    @Override
//    public void setConf(Configuration configuration) {
//        conf = configuration;
//    }
//
//    @Override
//    public Configuration getConf() {
//        return conf;
//    }
//
//    public static void main(String[] args) throws Exception {
//        int res = ToolRunner.run(new IDMappingStep1(), args);
//        System.exit(res);
//    }
//}
