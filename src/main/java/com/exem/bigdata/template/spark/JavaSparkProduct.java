package com.exem.bigdata.template.spark;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

public final class JavaSparkProduct extends AbstractJob {

    private Map<String, String> params;

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        addOption("appName", "n", "Spark Application", "Spark Application (" + DateUtils.getCurrentDateTime() + ")");

        params = parseArguments(args);
        if (params == null || params.size() == 0) {
            System.exit(APP_FAIL);
        }
        return SparkUtils.getSparkSessionForLocal(params.get("--appName"));
    }

    @Override
    protected void processing(SparkSession sparkSession) throws Exception {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> stringRDD = jsc.textFile("product.txt").persist(StorageLevel.MEMORY_AND_DISK());

        // CSV 파일을 로딩하여 Product RDD를 생성한다.
        JavaRDD<Product> products = stringRDD.map(new Function<String, Product>() {
            @Override
            public Product call(String row) throws Exception {
                String[] columns = StringUtils.splitPreserveAllTokens(row, ",");

                Product product = new Product();
                product.PRODUCT_CLASSIFICATION = columns[0];
                product.PRODUCT_NM = columns[1];
                product.BRAND_LINE = columns[2];
                product.GROUPED_KEY = product.PRODUCT_CLASSIFICATION + product.PRODUCT_NM + product.BRAND_LINE;

                return product;
            }
        });

        System.out.println(products.count());

        // Product RDD를 Group By한다.
        JavaPairRDD<String, Iterable<Product>> pairRDD = products.groupBy(new Function<Product, String>() {
            @Override
            public String call(Product product) throws Exception {
                return product.GROUPED_KEY;
            }
        });

        System.out.println(pairRDD.count());
        System.out.println(pairRDD.collectAsMap());

        // Group By한 Product RDD 집합을 조건에 맞는 것들을 필터링한다.
        JavaPairRDD<String, List<Product>> stringListJavaPairRDD = pairRDD.mapValues(new Function<Iterable<Product>, List<Product>>() {
            @Override
            public List<Product> call(Iterable<Product> products) throws Exception {
                List<Product> list = new ArrayList<Product>();
                Iterator<Product> iterator = products.iterator();
                while (iterator.hasNext()) {
                    Product product = iterator.next();
                    // 여기는 확장되어야 한다.
                    if (product.GROUPED_KEY.equals("101112")) list.add(product);
                }
                return list;
            }
        });

        // Group By한 데이터에서 건수가 0인것을 제외하고 모두 합친다.
        List<List<Product>> collected = stringListJavaPairRDD.values().collect();
        List<Product> finalProducts = new ArrayList();
        for (List<Product> p : collected) {
            if (p.size() > 0) {
                finalProducts.addAll(p);
            }
        }
        System.out.println(finalProducts);
    }

    public static void main(String[] args) throws Exception {
        new JavaSparkProduct().run(args);
    }

}
