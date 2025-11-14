import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HbaseSparkProcessSum {

    public void createHbaseTable() {
        Configuration config = HBaseConfiguration.create();

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")
                .setMaster("local[4]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        config.set(TableInputFormat.INPUT_TABLE, "products");

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(
                        config,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class);

        // 1) Nombre denregistrements
        long count = hBaseRDD.count();
        System.out.println("nombre d'enregistrements: " + count);

        // 2) Somme des ventes (somme de cf:price)
        JavaRDD<Result> resultRDD = hBaseRDD.values();

        JavaRDD<Integer> priceRDD = resultRDD.map(r -> {
            byte[] priceBytes = r.getValue(
                    Bytes.toBytes("cf"),      // famille de colonnes
                    Bytes.toBytes("price"));  // qualificateur

            if (priceBytes == null) {
                return 0;
            }

            try {
                String priceStr = Bytes.toString(priceBytes);
                return Integer.parseInt(priceStr);
            } catch (NumberFormatException e) {
                return 0;
            }
        });

        int totalSales = 0;
        if (!priceRDD.isEmpty()) {
            totalSales = priceRDD.reduce(Integer::sum);
        }

        System.out.println("somme des ventes de tous les produits: " + totalSales);

        jsc.close();
    }

    public static void main(String[] args) {
        HbaseSparkProcessSum admin = new HbaseSparkProcessSum();
        admin.createHbaseTable();
    }
}
