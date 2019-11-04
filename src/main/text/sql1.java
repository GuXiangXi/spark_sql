import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class sql1 {
    @Test
    public void text() {
        SparkSession spark = SparkSession.builder().appName("sql").master("local").config("spark.some.config.option", "some-value").getOrCreate();
        spark.sparkContext().setLogLevel("error");//设置log等级

        Dataset<Row> df = spark.read().json("C:/Users/gxx/Desktop/json.txt");
        //df.printSchema();//结构树状图
//        df.select("name").show();//单独展示name这一列
//        df.select("age".toUpperCase()).show();//将age放到第一列
//        df.groupBy("sex").count().show();//根据相同属性统计数量
//        df.dropDuplicates().show();//选择去重

        df.dropDuplicates().createOrReplaceTempView("user");
        //去重之后的结果
        System.out.println("去重之后的结果");
        spark.sql("select * from user").show();
        //找到最大年纪
        System.out.println("找到最大年纪");
        spark.sql("select max(age) from user").show();
        //求出平均年纪
        System.out.println("求出平均年纪");
        spark.sql("select avg(age) from user").show();
        //求出年纪总和
        System.out.println("求出年纪总和");
        spark.sql("select sum(age) from user").show();
        //求出性格比例
        Dataset<Row> boy = spark.sql("SELECT * FROM user  WHERE sex  =  '男'");
        Dataset<Row> girl=spark.sql("SELECT * FROM user  WHERE sex  =  '女'");
        System.out.println("男：女的比例为："+boy.count()+":"+girl.count());


//        df.show();

    }
}
