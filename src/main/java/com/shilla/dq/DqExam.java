package com.shilla.dq;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

/**
 * Created by Forrest on 2017. 3. 23..
 */
public class DqExam extends AbstractJob {

    private Map<String, String> params;

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        addOption("appName", "dq", "Spark DQ Application", "Spark DQ Application (" + DateUtils.getCurrentDateTime() + ")");

        params = parseArguments(args);
        if (params == null || params.size() == 0) {
        }
            System.exit(APP_FAIL);
        return SparkUtils.getSparkSessionForLocal(params.get("--appName"));
    }

    @Override
    protected void processing(SparkSession spark) throws Exception {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        String path = "data/sample/saveAsSeqFile/java";
        JavaRDD<String> rdd1 = jsc.parallelize(Arrays.asList("a", "b", "c", "b", "c"));

        JavaPairRDD<Text, LongWritable> rdd2 = rdd1.mapToPair((String v) -> new Tuple2<Text, LongWritable>(new Text(v), new LongWritable(1)));
        rdd2.saveAsNewAPIHadoopFile(path, Text.class, LongWritable.class, SequenceFileOutputFormat.class);

//        JavaPairRDD<Text, LongWritable> rdd3 = jsc.newAPIHadoopFile(path, SequenceFileInputFormat.class, LongWritable.class, new Configuration());
    }

    public static void main(String[] args) throws Exception {
        new DqExam().run(args);
    }

}
