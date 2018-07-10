package dataframe;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class DatasetConversion {

    public static String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    //
    // This must be a JavaBean in order for Spark to infer a schema for it
    //
    public static class Cust implements Serializable {
        private int id;
        private String name;
        private double sales;
        private double discount;
        private String state;
        private String date;

        public Cust(int id, String name, double sales, double discount, String state, String date) {
            this.id = id;
            this.name = name;
            this.sales = sales;
            this.discount = discount;
            this.state = state;
            this.date = date;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getSales() {
            return sales;
        }

        public void setSales(double sales) {
            this.sales = sales;
        }

        public double getDiscount() {
            return discount;
        }

        public void setDiscount(double discount) {
            this.discount = discount;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getDate() { return date; }

        public void setDate(String date) { this.date = date; }
    }

    //
    // A smaller JavaBean for a subset of the fields
    //
    public static class StateSales implements Serializable {
        private double sales;
        private String state;

        public StateSales(int id, String name, double sales, double discount, String state) {
            this.sales = sales;
            this.state = state;
        }

        public double getSales() {
            return sales;
        }

        public void setSales(double sales) {
            this.sales = sales;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion")
            .master("local[4]")
            .getOrCreate();

        //
        // The Java API requires you to explicitly instantiate an encoder for
        // any JavaBean you want to use for schema inference
        //
        Encoder<Cust> custEncoder = Encoders.bean(Cust.class);
        //
        // Create a container of the JavaBean instances
        //
        List<Cust> data = Arrays.asList(
                new Cust(1, "Widget Co", 120000.00, 0.00, "AZ", "2018-07-07 12:12:12"),
                new Cust(2, "Acme Widgets", 410500.00, 500.00, "CA", "2018-07-07 13:12:12"),
                new Cust(3, "Widgetry", 410500.00, 200.00, "CA", "2018-07-07 14:12:12"),
                new Cust(4, "Widgets R Us", 410500.00, 0.0, "CA", "2018-07-07 15:12:12"),
                // Adding erroneous row
                new Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA", "sfsdfsfsdf-07-07 16:12:12")
        );
        //
        // Use the encoder and the container of JavaBean instances to create a
        // Dataset
        //
        Dataset<Cust> ds = spark.createDataset(data, custEncoder);

        System.out.println("*** here is the schema inferred from the Cust bean");
        ds.printSchema();

        System.out.println("*** here is the data");
        ds.show();

        //quick way : row.getString(0) with give Datetime string
        FilterFunction<Row> filter = row -> validateDf(row.getString(0));
        Dataset<Row> filteredDataset = ds.toDF().filter(filter);

        System.out.println("*** filtered Data");
        filteredDataset.show();

        spark.stop();
    }

    private static Boolean validateDf(String dateStr) {
        try {
            LocalDateTime parsedDateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern(DATE_TIME_FORMAT));
            System.out.println("Parsed DateTime: " + parsedDateTime);
            return true;
        } catch(Exception ex) {
            System.out.println(String.format("Unable to parse the date time for %s", dateStr));
            return false;
        }
    }
}
