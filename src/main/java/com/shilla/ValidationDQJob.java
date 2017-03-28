package shilla;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DataTypeUtils;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import com.google.common.base.Joiner;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ValidationDQJob extends AbstractJob {

    private static Map<String, String> params;

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        addOption("appName", "n", "Validation DQ Spark Job", "Validation DQ Spark Job (" + DateUtils.getCurrentDateTime() + ")");

        params = parseArguments(args);
        if (params == null || params.size() == 0) {
            System.exit(APP_FAIL);
        }
        return SparkUtils.getSparkSessionForLocal(params.get("--appName"));
    }

    @Override
    protected void processing(SparkSession session) throws Exception {
        String tableName = "C_SBPR_BRDTB";
        Table table = getTable(tableName);
        String hdfsPath = getHDFSPathFromTableName(tableName);
        StructType schema = getColumnsFromTable(tableName);
        Dataset<Row> rows = session.read().schema(schema).option("delimiter", table.getDelimiter()).csv(hdfsPath);

        DQDefinition dq = getDQDefinition("DQ_123345");
        Map<String, String> functionMap = dq.getFunctionMap();
        Dataset<String> map = rows.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                List values = new ArrayList();
                int length = value.length();
                for (int i = 0; i < length; i++) {
                    Object obj = value.get(i);
                    String fieldName = schema.fieldNames()[i];
                    if (functionMap.get(fieldName) != null) {
                        boolean evaluate = getFunction(functionMap.get(fieldName)).evaluate(obj);
                        if (evaluate) {
                            values.add("[INVALID] " + obj);
                        } else {
                            values.add(obj == null ? "NULL" : obj);
                        }
                    } else {
                        values.add(obj == null ? "NULL" : obj);
                    }
                }
                System.out.println(Joiner.on(table.delimiter).join(values));
                return Joiner.on(table.delimiter).join(values);
            }
        }, Encoders.STRING());

        // 결과를 화면에 출력
        List<String> resultRows = map.collectAsList();
        System.out.println();
        for (String s : resultRows) {
            System.out.println(s);
        }
        System.out.println();
    }

    /**
     * Mock Function
     */
    public Function getFunction(String functionName) {
        return new Function();
    }

    /**
     * DB에서 DQ 테이블 정보
     */
    public DQDefinition getDQDefinition(String definitionId) {
        DQDefinition def = new DQDefinition();
        HashMap colsDef = new HashMap();
        colsDef.put("BRAND_ABBR_EN", "isNull");
        colsDef.put("LAST_ROWID_SYSTEM", "isNull");
        def.setFunctionMap(colsDef);
        return def;
    }

    /**
     * DB에서 테이블(Legacy에서 Sqoop 이관) 정보
     */
    public Table getTable(String tableName) {
        Table table = new Table();
        table.setHdfsPath("src/main/resources/mdm/BRDTB_170321.csv");
//        table.hdfsPath = "src/main/resources/mdm/yyyy=2017/mm=03/dd=21";
        table.setDelimiter(",");
        table.setTableName("C_SBPR_BRDTB");
        table.setColumns(getColumns(tableName));
        return table;
    }

    /**
     * DB에 있는 테이블의 컬럼 정보
     */
    public List<Column> getColumns(String tableName) {
        List<Column> cols = new ArrayList<Column>();

        cols.add(new Column("ROWID_OBJECT", "STRING", true));
        cols.add(new Column("CREATOR", "STRING", true));
        cols.add(new Column("CREATE_DATE", "STRING", true));
        cols.add(new Column("UPDATED_BY", "STRING", true));
        cols.add(new Column("LAST_UPDATE_DATE", "STRING", true));
        cols.add(new Column("CONSOLIDATION_IND", "STRING", true));
        cols.add(new Column("DELETED_IND", "STRING", true));
        cols.add(new Column("DELETED_BY", "STRING", true));
        cols.add(new Column("DELETED_DATE", "STRING", true));
        cols.add(new Column("LAST_ROWID_SYSTEM", "STRING", true));
        cols.add(new Column("DIRTY_IND", "STRING", true));
        cols.add(new Column("INTERACTION_ID", "STRING", true));
        cols.add(new Column("HUB_STATE_IND", "STRING", true));
        cols.add(new Column("CM_DIRTY_IND", "STRING", true));
        cols.add(new Column("BRAND_CD", "STRING", true));
        cols.add(new Column("BRAND_COMPANY_CD", "STRING", true));
        cols.add(new Column("BRAND_COMPANY_NM", "STRING", true));
        cols.add(new Column("USE_FG", "STRING", true));
        cols.add(new Column("IMP_DOM_INDICATOR", "STRING", true));
        cols.add(new Column("BRAND_ABBR", "STRING", true));
        cols.add(new Column("BRAND_ABBR_EN", "STRING", true));
        cols.add(new Column("EAN_USE_FLAG", "STRING", true));
        cols.add(new Column("SEASON_FLAG", "STRING", true));
        cols.add(new Column("CRE_ID", "STRING", true));
        cols.add(new Column("CRE_TIMESTAMP", "STRING", true));
        cols.add(new Column("UDT_ID", "STRING", true));
        cols.add(new Column("UDT_TIMESTAMP", "STRING", true));
        return cols;
    }

    public String getHDFSPathFromTableName(String tableName) {
        return getTable(tableName).getHdfsPath();
    }

    public StructField getStructFieldFromColumn(String columnName, String type, boolean nullable) {
        return DataTypes.createStructField(columnName, DataTypeUtils.getDataType(type), nullable);
    }

    public StructType getColumnsFromTable(String tableName) {
        Table table = getTable(tableName);
        List<Column> cols = table.getColumns();
        List<StructField> columns = new ArrayList<>();
        for (Column col : cols) {
            StructField sf = getStructFieldFromColumn(col.getName(), col.getType(), col.isNullable());
            columns.add(sf);
        }
        return DataTypes.createStructType(columns);
    }

    public static void main(String[] args) throws Exception {
        new ValidationDQJob().run(args);
    }

}
