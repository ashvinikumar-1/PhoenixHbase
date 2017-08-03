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
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by ashvinikumar on 31/7/17.
 */
public class Read {
    public static void main(String args[]){
        SparkConf sparkConf = new SparkConf().setAppName("practice").setMaster("local[*]");
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, "employee");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaPairRDD<ImmutableBytesWritable, Result> source = jsc
//                .newAPIHadoopRDD(conf, TableInputFormat.class,
//                        ImmutableBytesWritable.class, Result.class);
//        Result result = Result.EMPTY_RESULT;
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = javaSparkContext.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<Employee> studentRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, Employee  >() {
            private static final long serialVersionUID = -2021713021648730786L;
            public Employee  call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                Employee  bean = new Employee  ();
                try {
                    Result result = tuple._2;
                    //bean.setSid(sid);
                    bean.setName(Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("firstName"))));
                    bean.setAddress(Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("lastName"))));
                   // bean.setBranch(Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("branch"))));
                   // bean.setEmailId(Bytes.toString(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("emailId"))));
                    return bean;
                } catch(Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        });

        System.out.print("value="+studentRDD);



    }
}
