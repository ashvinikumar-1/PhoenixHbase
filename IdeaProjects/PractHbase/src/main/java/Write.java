import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.io.IOException;


/**
 * Created by ashvinikumar on 31/7/17.
 */
public class Write {


    private final static String tableName = "user";
    private final static String columnFamily = "details";

    private static JavaSparkContext sc;
    public static void main(String args[]) throws IOException {


        SparkSession sparkSession = SparkSession
                .builder()
                .appName("get HBase data")
                .config("spark.master", "local[*]")
                .getOrCreate();

        sc = new JavaSparkContext(args[0], "get HBase data");
        Configuration conf = HBaseConfiguration.create();

        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        // new Hadoop API configuration
        Job newAPIJobConfiguration = Job.getInstance(conf);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        HBaseAdmin hBaseAdmin = null;
        try {
            hBaseAdmin = new HBaseAdmin(conf);
            if (hBaseAdmin.isTableAvailable(tableName)) {
                System.out.println("Table " + tableName + " is available.");
            }
            else {
                System.out.println("Table " + tableName + " is not available.");
            }
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            hBaseAdmin.close();
        }


        System.out.println("-----------------------------------------------");
        readTableJava(conf);

        System.out.println("-----------------------------------------------");
        //writeRow
       // writeRowNewHadoopAPI(newAPIJobConfiguration.getConfiguration(),sparkSession);

        System.out.println("-----------------------------------------------");
        readTableJava(conf);

        System.out.println("-----------------------------------------------");
        //System.exit(0);
        sc.stop();

    }


    private static void readTableJava(Configuration conf) {

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                sc
                        .newAPIHadoopRDD(
                                conf,
                                TableInputFormat.class,
                                org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
                                org.apache.hadoop.hbase.client.Result.class)
                ;
        long count = hBaseRDD.count();

        System.out.println("Number of register in hbase table: " + count);
		//sc.stop();
    }

    private static void writeRowNewHadoopAPI(Configuration conf,SparkSession sparkSession) {

        StructType schema = new StructType(
                new StructField[] { new StructField("num", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("state", DataTypes.StringType, true, Metadata.empty()),

                });


        Dataset<Row> records =
                sparkSession.read().format("csv").schema(schema).option("header", "false")
                        .option("delimiter", ",").load("/home/ashvinikumar/newkeyValuePair.csv");
        //JavaRDD Rrecords = sc.textFile("/home/ashvinikumar/keyValuePair.csv");
        JavaRDD<Row> record = records.toJavaRDD();


        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = record.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Row row)
                    throws Exception {
                //String str[] = t.split(",");
                Put put = new Put(Bytes.toBytes(row.getString(0)));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"),
                        Bytes.toBytes(row.getString(1)));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("address"),
                        Bytes.toBytes(row.getString(2)));

                return new Tuple2<ImmutableBytesWritable, Put>(
                        new ImmutableBytesWritable(), put);
            }
        });

        hbasePuts.saveAsNewAPIHadoopDataset(conf);
        //sc.stop();

    }
}
