//package com.iflytek.idmapping.orc;
//
//import com.iflytek.idmapping.util.IdMappingUtil;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.orc.mapred.OrcMap;
//import org.apache.orc.mapred.OrcStruct;
//import org.apache.orc.mapreduce.OrcInputFormat;
//import org.apache.orc.mapreduce.OrcOutputFormat;
//import org.apache.orc.TypeDescription;
//
//import java.io.IOException;
//import java.util.Map;
//
///**
// * Created by admin on 2017/5/18.
// */
//public  class MappingStep1 {
//    public static final Log log = LogFactory.getLog(MappingStep1.class);
//
//    public static class Step1Mapper extends Mapper<NullWritable, OrcStruct, Text, OrcStruct> {
//        public void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
//
//            OrcStruct lowerOrc = IDsUtils.toLowCase(value);
//            lowerOrc.setFieldValue(0, new Text(""));
//            OrcMap<Text, IntWritable> orcMap = IDsUtils.mergeIDs2Map(lowerOrc);
//
//            if (orcMap != null && orcMap.size() != 0) {
//                String secondKey = orcMap.keySet().toArray()[0].toString() +
//                        orcMap.keySet().toArray()[orcMap.keySet().size() - 1].toString()
//                        + IdMappingUtil.getRandomString(context.getTaskAttemptID().toString(), Integer.MAX_VALUE);
//                lowerOrc.setFieldValue(0, new Text(secondKey));
//                for (Map.Entry<Text, IntWritable> entry : orcMap.entrySet()) {
//                    Text idValue = entry.getKey();
//                    context.write(idValue, lowerOrc);
////                    context.write(NullWritable.get(), lowerOrc);
//                }
//            }
//        }
//    }
//
//    public static class Step1Reducer extends Reducer<Text, OrcStruct, NullWritable, OrcStruct> {
//        //        //具体OrcStruct字段对应hadoop的定义参考https://orc.apache.org/docs/mapreduce.html
////        private TypeDescription schema = TypeDescription.fromString(IDsUtils.IDsSchemaStr);
////        private OrcStruct orcs = (OrcStruct) OrcStruct.createValue(schema);
////        private static final NullWritable nw = NullWritable.get();
//
//        public  void reduce(Text key, Iterable<OrcStruct> values, Context context) throws IOException, InterruptedException {
//            for (OrcStruct val : values) {
//                context.write(NullWritable.get(), val);
//                break;
//            }
//        }
//    }
//
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = new Configuration();
//        //要设置结构，否则reduce会提示输入空值
//        //conf.set("orc.mapred.output.schema","struct<account:string,domain:string,post:string>");
//        conf.set("orc.mapred.output.schema", IDsUtils.IDsSchemaStr);
//        System.out.println("input:" + args[0]);
//        System.out.println("output:" + args[1]);
//        Job job = new Job(conf, "ORC Write Test");
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setJarByClass(MappingStep1.class);
//        job.setMapperClass(Step1Mapper.class);
//        job.setReducerClass(Step1Reducer.class);
//
//        job.setInputFormatClass(OrcInputFormat.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(OrcStruct.class);
//
//      //  job.setOutputFormatClass(OrcOutputFormat.class);
//        job.setOutputFormatClass(OrcOutputFormat.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(OrcStruct.class);
//
//        //reduce类型设置
//        job.setNumReduceTasks(50);
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}
