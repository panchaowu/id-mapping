package com.iflytek.idmapping.hbase;

import com.iflytek.idmapping.util.OrcUtil;
import com.iflytek.idmapping.zookeeper.BaseZookeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by admin on 2017/6/23.
 */
public class LoadIDs2HBase implements Tool {

    public static Configuration conf;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class LoadIDsMapper extends Mapper<NullWritable, Writable, ImmutableBytesWritable, KeyValue> {
        byte[] family = Bytes.toBytes("ids");
        byte[] qualifier = Bytes.toBytes("value");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
        }

        public static void toHBaseMap(Map<Text,IntWritable> typeMap, Map<String,Integer> hbaseMap) {
            if(hbaseMap != null && typeMap != null && typeMap.size() > 0) {
                for(Map.Entry<Text,IntWritable> e : typeMap.entrySet()) {
                    hbaseMap.put(e.getKey().toString(),e.getValue().get());
                }
            }
        }

        public static String orc2IDsHBaseValue(OrcStruct orcs) {
            IDsHBaseValue value = new IDsHBaseValue();
            value.init();
            String globalId = OrcUtil.getIDsGlobalId(orcs);
            value.setGlobal_id(globalId);
            for(int i = 1; i <= OrcUtil.typeCount; i++) {
                Map<String,Integer> mMap = value.getIndexMap(i);
                Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)OrcUtil.idsInspector.getStructFieldData(orcs,OrcUtil.idsFields.get(i));
                toHBaseMap(typeMap,mMap);
            }
            return value.toString();
        }

        @Override
        protected void map(NullWritable key, Writable value, Context context) throws IOException, InterruptedException {
            OrcStruct idsData = (OrcStruct) value;
            String globalId = OrcUtil.getIDsGlobalId(idsData);
            String hbaseValue = orc2IDsHBaseValue(idsData);
            byte[] rowKey = Bytes.toBytes(globalId);
            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
            byte[] hValue = Bytes.toBytes(hbaseValue);
            Put put=new Put(rowKey);
            put.addColumn(family, qualifier, hValue);
           // context.write(rowKeyWritable,put);
            KeyValue kv  = new KeyValue(rowKey,family,qualifier,hValue);
            context.write(rowKeyWritable,kv);
        }
    }

    public  static String padLeft(String s, int length)
    {
        byte[] bs = new byte[length];
        byte[] ss = s.getBytes();
        Arrays.fill(bs, (byte) (48 & 0xff));
        System.arraycopy(ss, 0, bs,length - ss.length, ss.length);
        return new String(bs);
    }

    public static byte[][] getHexSplits(String startKey, String endKey,
                                        int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];

        int lowestKey = Integer.parseInt(startKey,16);
        int highestKey = Integer.parseInt(endKey,16);
        //int range = highestKey - lowestKey;
        int increment = (highestKey - lowestKey)/numRegions;
        for (int i = 0; i < numRegions - 1; i++) {
            int key = lowestKey + (increment * (i+1));
            String hexKey = padLeft(Integer.toHexString(key),4).toUpperCase();
          //  System.out.println(hexKey);
            byte[] b = hexKey.getBytes();
            splits[i] = b;
        }
        return splits;
    }

    @Override
    public int run(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];
        System.out.println("input"+ inputPath);
        System.out.println("output"+ inputPath);
        System.out.println("tableName"+ inputPath);
        // zookeeper 操作
//        BaseZookeeper zoo = new BaseZookeeper();
//        zoo.connectZookeeper("10.10.12.82,10.10.12.83,10.10.12.84");
//        String inHBaseUse = zoo.getData("/idmapping/active_ids2hbase");
//        String tableName = "";
//        if(inHBaseUse.equals("idmapping_ids2hbase_1")) {
//            tableName = "idmapping_ids2hbase_2";
//        } else if(inHBaseUse.equals("idmapping_ids2hbase_2")) {
//            tableName = "idmapping_ids2hbase_1";
//        } else {
//            System.out.println("inHBaseUse" + tableName + "is invalid!");
//            zoo.closeConnection();
//            return 0;
//        }

        conf = HBaseUtil.conf;
        byte[][] splits = getHexSplits("0000","FFFF",800);
        HBaseUtil.createIDsTable(tableName,splits);
        Job job = new Job(conf,"ids2hbase");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(LoadIDsMapper.class);
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setOutputFormatClass(HFileOutputFormat2.class);
        job.setNumReduceTasks(0);
        job.setJarByClass(LoadIDs2HBase.class);
        HTable table = new HTable(conf, tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table);

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        if(ret == 0) {
            System.out.println("chmod :" + outputPath);
            Runtime.getRuntime().exec("hadoop fs -chmod -R 777 " + outputPath);
            System.out.println("start bulk load ...");
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
            loadFfiles.doBulkLoad(new Path(outputPath), table);//导入数据
            System.out.println("Bulk Load Completed ...");
           // zoo.setData("/idmapping/active_ids2hbase",tableName);
        }
       // zoo.closeConnection();
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LoadIDs2HBase(), args);
        System.exit(res);
    }

}
