package com.iflytek.idmapping.Index;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.util.List;
import java.util.Properties;

/**
 * Created by admin on 2017/6/5.
 */
public class IndexOrcUtil {
    public static final String indexOrcSchema = "struct<id:string,global_id:string>";

    public static SettableStructObjectInspector indexInspector;
    public static List<? extends StructField> indexFields;
    public static OrcSerde indexSerde;


    public static void setupIndexOrc() {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(indexOrcSchema);
        indexInspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        indexFields = indexInspector.getAllStructFieldRefs();
    }

    public static void initializeIndex(TaskInputOutputContext context) {
        indexSerde = new OrcSerde();
        StringBuilder colums = new StringBuilder();
        StringBuilder types = new StringBuilder();
        for (StructField field : indexFields) {
            colums.append(field.getFieldName());
            colums.append(",");
            types.append(field.getFieldObjectInspector().getTypeName());
            types.append(",");
        }
        colums.deleteCharAt(colums.length() - 1);
        types.deleteCharAt(types.length() - 1);
        Properties properties = new Properties();
        properties.put(IOConstants.COLUMNS, colums.toString());
        properties.put(IOConstants.COLUMNS_TYPES, types.toString());
        indexSerde.initialize(context.getConfiguration(), properties);
    }

    public static OrcStruct initIndex() {
        OrcStruct os = (OrcStruct)indexInspector.create();
        indexInspector.setStructFieldData(os,indexFields.get(0),new Text(""));
        indexInspector.setStructFieldData(os,indexFields.get(1),new Text(""));
        return os;
    }

    public static void setIndexId(OrcStruct os,String id) {
        indexInspector.setStructFieldData(os,indexFields.get(0),new Text(id));
    }

    public static void setIndexGlobalId(OrcStruct os,String globalId) {
        indexInspector.setStructFieldData(os,indexFields.get(1),new Text(globalId));
    }

    public static String getIndexId(OrcStruct os) {
        return indexInspector.getStructFieldData(os,indexFields.get(0)).toString();
    }

    public static String getIndexGlobalId(OrcStruct os) {
        return indexInspector.getStructFieldData(os,indexFields.get(1)).toString();
    }

}
