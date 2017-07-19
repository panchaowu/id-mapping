//package com.iflytek.idmapping.hbase;
//
//import com.iflytek.idmapping.util.OrcUtil;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
//import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import java.io.IOException;
//import java.math.BigInteger;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by admin on 2017/6/13.
// */
//public class IDs2HBase implements Tool {
//
//    public static Configuration conf;
//
//    @Override
//    public void setConf(Configuration configuration) {
//        this.conf = configuration;
//    }
//
//    @Override
//    public Configuration getConf() {
//        return conf;
//    }
//
//    public static class ImportIDsMapper extends Mapper<NullWritable, Writable, ImmutableBytesWritable, Put> {
//        byte[] family = Bytes.toBytes("ids");
//        byte[] qualifier = Bytes.toBytes("value");
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            OrcUtil.setupIDsOrc();
//        }
//
//        public static void toHBaseMap(Map<Text,IntWritable> typeMap,Map<String,Integer> hbaseMap) {
//            if(hbaseMap != null && typeMap != null && typeMap.size() > 0) {
//                for(Map.Entry<Text,IntWritable> e : typeMap.entrySet()) {
//                    hbaseMap.put(e.getKey().toString(),e.getValue().get());
//                }
//            }
//        }
//
//        public static String orc2IDsHBaseValue(OrcStruct orcs) {
//            IDsHBaseValue value = new IDsHBaseValue();
//            value.init();
//            String globalId = OrcUtil.getIDsGlobalId(orcs);
//            value.setGlobal_id(globalId);
//            for(int i = 1; i <= OrcUtil.typeCount; i++) {
//                Map<String,Integer> mMap = value.getIndexMap(i);
//                Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)OrcUtil.idsInspector.getStructFieldData(orcs,OrcUtil.idsFields.get(i));
//                toHBaseMap(typeMap,mMap);
//            }
//            return value.toString();
//        }
//
//        @Override
//        protected void map(NullWritable key, Writable value, Context context) throws IOException, InterruptedException {
//            OrcStruct idsData = (OrcStruct) value;
//            String globalId = OrcUtil.getIDsGlobalId(idsData);
//            String hbaseValue = orc2IDsHBaseValue(idsData);
//            byte[] rowKey = Bytes.toBytes(globalId);
//            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
//            byte[] hValue = Bytes.toBytes(hbaseValue);
//            Put put=new Put(rowKey);
//            put.addColumn(family, qualifier, hValue);
//            context.write(rowKeyWritable,put);
//        }
//    }
//
//    public  static String padLeft(String s, int length)
//    {
//        byte[] bs = new byte[length];
//        byte[] ss = s.getBytes();
//        Arrays.fill(bs, (byte) (48 & 0xff));
//        System.arraycopy(ss, 0, bs,length - ss.length, ss.length);
//        return new String(bs);
//    }
//
//    public static byte[][] getHexSplits(String startKey, String endKey,
//                                        int numRegions) {
//        byte[][] splits = new byte[numRegions - 1][];
//
//        //BigInteger lowestKey = new BigInteger(startKey, 16);
//        //BigInteger highestKey = new BigInteger(endKey, 16);
//        //BigInteger range = highestKey.subtract(lowestKey);
//        int lowestKey = Integer.parseInt(startKey,16);
//        int highestKey = Integer.parseInt(endKey,16);
//        int range = highestKey - lowestKey;
//        int increment = (highestKey - lowestKey)/numRegions;
//        for (int i = 0; i < numRegions - 1; i++) {
//            int key = lowestKey + (increment * (i+1));
//            String hexKey = padLeft(Integer.toHexString(key),4).toUpperCase();
//            byte[] b = hexKey.getBytes();
//            splits[i] = b;
//        }
//        return splits;
//    }
//
//    @Override
//    public int run(String[] args) throws Exception {
//
//        String inputPath = args[0];
//        String tableName = args[1];
//
//        conf = HBaseUtil.conf;
//
//        byte[][] splits = getHexSplits("0000","FFFF",500);
//        HBaseUtil.createIDsTable(tableName,splits);
//        Job job = new Job(conf,"ids2hbase");
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//
//        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
//
//        job.setInputFormatClass(OrcNewInputFormat.class);
//
//        job.setOutputKeyClass(ImmutableBytesWritable.class);
//        job.setOutputValueClass(Put.class);
//        job.setOutputFormatClass(TableOutputFormat.class);
//        job.setNumReduceTasks(0);
//
//        job.setMapperClass(ImportIDsMapper.class);
//        job.setNumReduceTasks(0);
//        job.setJarByClass(IDs2HBase.class);
//        return (job.waitForCompletion(true) ? 0 : 1);
//    }
//
//    public static void main(String[] args) throws Exception {
//        int res = ToolRunner.run(new IDs2HBase(), args);
//        System.exit(res);
//    }
//}
