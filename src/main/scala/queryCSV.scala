import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object queryCSV{
    def main(args:Array[String]):Unit = {
        val spark =
            SparkSession
            .builder
            .appName("teamA")
            .config("spark.master", "local")
            .config("spark.eventLog.enabled", "false")
            .getOrCreate()
            
        
        val teamA = spark.read
            .format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("inputs/UpdatedNoNull.csv")
        
        val teamA1=spark.read
            .format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("inputs/ModifiedTime.csv")

         val timeSales = spark.read.format("csv")
            .option("inferSchema","true")
            .option("header", "true")
            .load("inputs/salesPerCountry.csv")
        
        teamA.createOrReplaceTempView("market_data")
        teamA1.createOrReplaceTempView("market_data1")
        timeSales.createOrReplaceTempView("timeSales")

        
        //q5. Top 10 countries in sales
        def TopCountriesSales(spark:SparkSession): DataFrame  = {
        return spark.sql("SELECT Country,ROUND(SUM((Qty * Price)),2) as Sales FROM market_data WHERE Payment_txn_success = 'Y' GROUP BY Country order by Sales desc limit 10")
        }

        val q5 = TopCountriesSales(spark).coalesce(1)
        q5.write.mode("overwrite").csv("outputs/TopCountriesSales")
        q5.show()

        //q6. The popular product category in China for sales with the payment succeed.
        def PopularProductCatChina(spark:SparkSession): DataFrame  = {
        return spark.sql("SELECT Product_category, sum(qty) as QTY2  FROM market_data WHERE Payment_txn_success = 'Y' and country = 'China' group by Product_category order by QTY2 desc")
        }

        val q6 = PopularProductCatChina(spark).coalesce(1)
        q6.write.mode("overwrite").csv("outputs/PopularProductCatChina")
        q6.show()
        
        //q7. Most popular payment type per country
        def PopularPaymentType(spark:SparkSession): DataFrame  = {
        return spark.sql("SELECT country, payment_type, count(*) as numbers FROM market_data WHERE Payment_txn_success = 'Y' group by country, payment_type order by country")
        }

        val q7 = PopularPaymentType(spark).coalesce(1)
        q7.write.mode("overwrite").csv("outputs/PopularPaymentType")
        q7.show()

        //q8. Most popular product per country
        def MostPopularProduct(spark:SparkSession): DataFrame  = {
        return spark.sql("select CountryTable.country, CatTable.product_name, CatTable.qty2 from (select country, product_name, max(QTY) as qty2 from (select country, product_name, max(qty) as QTY from " +
            "(select country, product_name, sum(qty) as qty from (select country, Product_name, qty from ((select * from market_data where  Payment_txn_success = 'Y') as M_data) ) as T group by country, product_name) as T1 group by country, product_name) as T2 group by product_name, country) as CatTable inner join (select country, max(QTY) as qty2 from (select country, product_name, max(qty) as QTY from " +
            "(select country, product_name, sum(qty) as qty from (select country, Product_name, qty from ((select * from market_data where  Payment_txn_success = 'Y') as M_data) ) as T group by country, product_name) as T1 group by country, product_name) as T2 group by country) as CountryTable on CountryTable.qty2 = CatTable.qty2 order by CatTable.qty2")
        }

        val q8 = MostPopularProduct(spark).coalesce(1)
        q8.write.mode("overwrite").csv("outputs/MostPopularProduct")
        q8.show()
        
        //q9. QTY Over Time(No transactions from 1pm Until 1AM)
        def QTY_Time(spark:SparkSession): DataFrame  = {
        return spark.sql("SELECT sum(QTY), Time FROM market_data WHERE Payment_txn_success = 'Y' GROUP BY Time")
        }

        val q9 = QTY_Time(spark).repartition(1)
        q9.write.mode("overwrite").csv("Outputs/QTY_Time")
        q9.show()

        //q.10 QTY OVER DATE(showing downtrend with 2021 as an anomaly)
        def QTY_DATE(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT sum(QTY), Date FROM market_data1 WHERE Payment_txn_success = 'Y' GROUP BY Date")
        }

        val q10 = QTY_DATE(spark).repartition(1)
        q10.write.mode("overwrite").csv("Outputs/QTY_DATE")

        //q11. CATEGORY OVER TIME (FOOD HAS REMAINED THE HIGHEST QUANTITY CATEGORY SOLD THROUGHOUT ALL YEARS REVIEWED)
        def CAT_DATE(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT sum(QTY), Product_category, DATE FROM market_data1 WHERE Payment_txn_success = 'Y' GROUP BY Product_category, DATE")
        }

        val q11 = CAT_DATE(spark).repartition(1)
        q11.write.mode("overwrite").csv("Outputs/CAT_DATE")

        //q12. TOP 5 PRODUCTS PER YEAR
        def Top2018(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT sum(QTY), Product_name, DATE From market_data1 WHERE Payment_txn_success = 'Y'AND DATE=2018 GROUP BY Product_name, DATE ORDER BY sum(QTY) desc limit 5")
        }

        def Top2019(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT sum(QTY), Product_name, DATE From market_data1 WHERE Payment_txn_success = 'Y'AND DATE=2019 GROUP BY Product_name, DATE ORDER BY sum(QTY) desc limit 5")
        }

        def Top2020(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT sum(QTY), Product_name, DATE From market_data1 WHERE Payment_txn_success = 'Y'AND DATE=2020 GROUP BY Product_name, DATE ORDER BY sum(QTY) desc limit 5")
        }

        def Top2021(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT sum(QTY), Product_name, DATE From market_data1 WHERE Payment_txn_success = 'Y'AND DATE=2021 GROUP BY Product_name, DATE ORDER BY sum(QTY) desc limit 5")
        }

        val datejoin = Top2018(spark).union(Top2019(spark)).union(Top2020(spark)).union(Top2021(spark)).repartition(1)
        //datejoin.show(false)
        datejoin.write.mode("overwrite").csv("Outputs/TOP_5_PRODUCTS")

        //q13. TIME OF DAY WITH HIGHEST TRANSACTIONS
        def TOP_TIME_TRANSACTIONS(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT count(product_name) as Transactions, Time From market_data1 WHERE Payment_txn_success = 'Y' group by Time Order by Transactions Desc")
        }

        val q13 = TOP_TIME_TRANSACTIONS(spark).repartition(1)
        q13.write.mode("overwrite").csv("Outputs/TOP_TIME_TRANSACTIONS")
        
        //q14. DATES WITH HIGHEST TRANSACTIONS - Top 30 Dates are all in January
        def Top_Day_Transactions(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT count(product_name) as Transactions, Date from market_data WHERE Payment_txn_success = 'Y' group by Date Order by Transactions Desc")
        }
        
        val q14=Top_Day_Transactions(spark).repartition(1)
        q14.write.mode("overwrite").csv("Outputs/Top_Day_Transactions")

        //q15. Type of product the customer bought
        def Type_Product_Purchases(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT Product_category, COUNT(Payment_txn_success) as total_trans FROM market_data WHERE Payment_txn_success == 'Y' GROUP BY Product_category ORDER BY total_trans LIMIT 10")
        }

        val q15=Type_Product_Purchases(spark).repartition(1)
        q15.write.mode("overwrite").csv("Outputs/Type_Product_Purchases")

        //q16. Which customer had the most successful transactions
        def Customer_Successful_Transactions(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT Customer_ID, Customer_name, Country, COUNT(Payment_txn_success) as total_trans FROM market_data WHERE Payment_txn_success == 'Y' GROUP BY Customer_ID, Customer_name, Country ORDER BY total_trans DESC LIMIT 10")
        }

        val q16=Type_Product_Purchases(spark).repartition(1)
        q16.write.mode("overwrite").csv("Outputs/Customer_Successful_Transactions")

        //q17. Which customer had the most transaction failures
        def Customer_Transaction_Failures(SPARK:SparkSession): DataFrame = {
        return spark.sql("SELECT Customer_ID, Customer_name, Country, COUNT(Payment_txn_success) as total_trans FROM market_data WHERE Payment_txn_success == 'N' GROUP BY Customer_ID, Customer_name, Country ORDER BY total_trans DESC LIMIT 10")
        }

        val q17=Customer_Transaction_Failures(spark).repartition(1)
        q17.write.mode("overwrite").csv("Outputs/Customer_Transaction_Failures")
        
        
        //q18. Top 15 successful wesbites in countries
        def Top15successWesbsites(spark:SparkSession): DataFrame = {
            return spark.sql("SELECT Ecomerence_website_name, COUNT(Payment_txn_success) AS Success,Country FROM market_Data WHERE Payment_txn_success = 'Y'  GROUP BY Country,Ecomerence_website_name limit 15")
        }

        val q18=Top15successWesbsites(spark).repartition(1)
        q18.write.mode("overwrite").csv("Outputs/Top15successWesbsites")


        //q19. Top 15 failure websites in countries
        def Top15failureWebsites(spark:SparkSession): DataFrame ={
            return spark.sql("SELECT Ecomerence_website_name,COUNT(Payment_txn_success) AS Failure ,Country FROM market_Data WHERE Payment_txn_success = 'N' GROUP BY Country,Ecomerence_website_name limit 15")
        }

        val q19=Top15failureWebsites(spark).repartition(1)
        q19.write.mode("overwrite").csv("Outputs/Top15failureWebsites")
        
        
        //q20. The websites with the that had the most successful transactions
        def MostSuccessfulTransactions(spark:SparkSession): DataFrame ={
            return spark.sql("SELECT Ecomerence_website_name,COUNT(Payment_txn_success) AS Transactions FROM market_Data WHERE Payment_txn_success ='Y' GROUP BY Ecomerence_website_name limit 15")
        }

        val q20=MostSuccessfulTransactions(spark).repartition(1)
        q20.write.mode("overwrite").csv("Outputs/MostSuccessfulTransactionsWesbites")
        
        //Sales Per Country
        def salesPerCountry(spark:SparkSession): DataFrame = {
            val salesPerCountryQuery= spark.sql("SELECT Country,ROUND(SUM((Qty * Price)),2) as Sales FROM market_data WHERE Payment_txn_success = 'Y' GROUP BY Country")
            return salesPerCountryQuery
        }

        val salesPerCountryCSV = salesPerCountry(spark).repartition(1)
        salesPerCountryCSV.write.mode("overwrite").csv("outputs/salesPerCountry")

        //AM and PM sales for each country
        def amPmSales(spark: SparkSession, timeSales: DataFrame): DataFrame = {
            val amPmSalesQuery = spark.sql("SELECT pmTable.Country, PMSales, AMSales FROM (SELECT Country, ROUND(SUM(Sales),2) as PMSales FROM timeSales WHERE Time LIKE '%PM' GROUP BY Country)pmTable JOIN (SELECT Country, ROUND(SUM(Sales),2) as AMSales FROM timeSales WHERE Time LIKE '%AM' GROUP BY Country)amTable ON pmTable.Country = amTable.Country")
            return amPmSalesQuery
        }

        val amPmSalesCSV = amPmSales(spark,timeSales).repartition(1)
        amPmSalesCSV.write.mode("overwrite").csv("outputs/AM_PM_Sales")

        //Number of Free items, under a dollar items, and sales for each website
        def websiteFreeQty (spark:SparkSession): DataFrame = {
            val freeQtyQuery = spark.sql("SELECT t4.Ecomerence_website_name, t4.TotalSales, UnderADollarQty, FreeQty FROM(SELECT t1.Ecomerence_website_name, UnderADollarQty, FreeQty FROM(SELECT Ecomerence_website_name, SUM(QTY) AS UnderADollarQty FROM market_data WHERE Price > 0.00 AND Price < 1.00 AND Payment_txn_success = 'Y' GROUP BY Ecomerence_website_name)t1 JOIN (SELECT Ecomerence_website_name, SUM(Qty) AS FreeQty FROM market_data WHERE Price = 0 AND Payment_txn_success = 'Y' GROUP BY Ecomerence_website_name)t2 ON t1.Ecomerence_website_name = t2.Ecomerence_website_name ORDER BY UnderADollarQty DESC)t3 JOIN (SELECT Ecomerence_website_name, ROUND(SUM(Qty * Price),2) AS TotalSales FROM market_data Group By Ecomerence_website_name)t4 ON t3.Ecomerence_website_name = t4.Ecomerence_website_name")
            return freeQtyQuery
        }

        val websiteFreeQtyCSV = websiteFreeQty(spark).repartition(1)
        websiteFreeQtyCSV.write.mode("overwrite").csv("outputs/websiteFreeQty")
            
        spark.stop()
    }
}