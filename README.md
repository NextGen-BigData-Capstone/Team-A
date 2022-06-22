# Big Data Analysis

This is a team A analysis project that queries the information taken from Khafka consumer and is used to decipher trends within a CSV file. This project incorporates graphs and charts that was created from the data collect by Khafka producer.


# Technologies Used

**VSCODE**
>- IDE utilized to query and collect data from Kafka consumer and producer

**CSV files**
>- The selected output for our queries to be saved to in order to visualize data

**SparkShell**
>- An additional method to query data using our Kafka Consumer

**Qlik**
>- A visual aid used to create graphs and charts for a PowerPoint presentation

**Google Slides**
>- Presentation tool

**Scala**
>- The language the program was built on

# Features
>-CSV files
>
>-Google Slides
>
>-Qlik
>
>-VSCODE
>
>-Spark-Shell
>
>-Spark.sql
>
>-Spark Build 
>
# To-do list
>-Add more trends and queries to VSCODE
>
>-Add Graphs and Charts to Google slides
>
>-Limit the amount of unecessary queries inside Google slides
>

# Getting Started

linux:
>git clone 
>git pull 
>![enter image description here](https://ucarecdn.com/bd26066f-2180-4d40-bab5-3024a950a955/ScreenShot20220622at100249AM.png)


VS Code: 
>Download Visual Studio Code and install the Scala extension.
>



>Install Spark on your machine.

# Usage

VSCode
>Run the main file in the Terminal.
>
>
Start ssh Localhost
Start HDFS Servers

>![enter image description here](https://ucarecdn.com/a0ee2163-6137-4f79-81b8-73fef82f9ee4/ScreenShot20220601at125037PM.png)
>
Enter Spark-Shell REPL 
>
>![enter image description here](https://ucarecdn.com/39d50981-4ce2-46cb-8241-ef33ef70dc14/ScreenShot20220622at95943AM.png)
>Run Queries to produce CSV Files
>
>**Query 1**
>>SELECT Customer_ID, Customer_name, Country, COUNT(Payment_txn_success) as total_trans FROM market_data WHERE Payment_txn_success == 'Y' GROUP BY Customer_ID, Customer_name, Country ORDER BY total_trans DESC LIMIT 10
>
>**Query 2**
>>SELECT Customer_ID, Customer_name, Country, COUNT(Payment_txn_success) as total_trans FROM market_data WHERE Payment_txn_success == 'N' GROUP BY Customer_ID, Customer_name, Country ORDER BY total_trans DESC LIMIT 10
>
>**Query 3**
>>"SELECT sum(QTY), Product_name, DATE From market_data1 WHERE Payment_txn_success = 'Y'AND DATE=2021 GROUP BY Product_name, DATE ORDER BY sum(QTY) desc limit 5")
>
>>
>
>>

Analyze data of CSV files with any data visualization tool you are comfortable with. 
>![Qlik](https://ucarecdn.com/3f7df306-adc0-4c28-a59b-fad088cc33a1/qlikvectorlogosmall.png)

>![R Studio](https://ucarecdn.com/08129904-899e-484d-99fb-197df7293ea8/R.png)
# Contributors
>- Anthony Feliciano - Khafka Consumer Team, Query/Trend Team
>- Devene Gayle - Visualization Team
>- Alexander Huang - Query/Trend Team 
>- Khoa La - Query/Trend Team,Khafka Consumer Team 
>- Kolby Lingerfelt - Query/Trend Team,Scrum Master
>- Jodi Mitchell - Query/Trend Team,Kafka Consumer Team
>- Keren Sangalaza -Visualization Team,Query/Trend Team
>- Mpagi Sendawula - Khafka Consumer Team, Query/Trend Team
>- Jerry Scott - Visualization Team
>- Kieran Vergara - Khafka Consumer Team,Query/Trend Team

