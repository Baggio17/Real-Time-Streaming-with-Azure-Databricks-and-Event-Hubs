# **Real-Time Streaming Analytics with Azure Databricks and Event Hubs**

## **Business Scenario**

This project showcases the implementation of an end-to-end real-time analytics solution leveraging the Medallion Architecture. By utilizing simulated weather data streamed via Azure Event Hubs, the solution ensures efficient ingestion, processing, and storage of streaming data. The pipeline is designed to meet modern data processing and governance standards while enabling near real-time reporting through Power BI dashboards.
![Azure Solution Architecture](https://github.com/user-attachments/assets/b780a25f-f940-4a60-a1f2-476cfc6179b2)

## **Business Requirements**

-Ingest simulated weather data in JSON format from Azure Event Hubs Data Explorer.

-Process raw streaming data using Databricks and Spark Structured Streaming.

-Transform and organize the data into Bronze, Silver, and Gold layers using the Medallion Architecture.

-Aggregate data for efficient reporting and analytics.

-Visualize key metrics like temperature trends and other weather parameters using Power BI.

## **Deliverables**

**Databricks Notebooks:** For real-time data ingestion, transformation, and storage.

**Processed Data:** High-quality datasets stored in Delta format across Bronze, Silver, and Gold layers.

**Power BI Dashboards:** Near real-time insights into weather trends.

## **Specification Details**

**Dataset:** Simulated weather data in JSON format with attributes such as temperature, humidity, wind speed, wind direction, and precipitation.

**Tools Used:** Azure Event Hubs, Azure Databricks, Spark Structured Streaming, Delta Lake, and Power BI.

## **Steps to Complete the Project**

### **1. Environment Setup**

Created an Azure Event Hubs namespace and Event Hub for data ingestion.

Configured an Azure Databricks workspace with Unity Catalog enabled for governance.

Set up a Databricks cluster with the necessary Spark runtime and libraries.

a.Single Node Compute Cluster: 12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)

b.Maven Library installed on Compute Cluster: com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
![Screenshot (99)](https://github.com/user-attachments/assets/49ed7866-50c0-4760-86a2-e5601b841f9d)

### **2. Medallion Architecture Implementation**

Bronze Layer: Stored raw JSON data for traceability.

Silver Layer: Cleaned and transformed data, converting JSON objects into tabular format.

Gold Layer: Aggregated data, such as average temperature and humidity, for efficient analytics.

### **3. Incremental Processing**

Configured checkpointing in Databricks to track data processing progress.

Designed fault-tolerant streams that can recover from failures.
![Screenshot (104)](https://github.com/user-attachments/assets/693c94ed-08f2-4592-b118-0d35a7f596a2)



### **4. Data Ingestion**
Simulated streaming weather data using Azure Event Hubs' built-in data Explorer.

Established a connection between Event Hubs and Databricks using the Azure Event Hubs Spark connector.

Ingested raw streaming data into the Bronze Layer.
![Screenshot (105)](https://github.com/user-attachments/assets/c54588ae-4efe-4c50-97ee-8ae309175e80)
![Screenshot (101)](https://github.com/user-attachments/assets/9fc8be4f-c384-4f61-a353-118101ce1d61)


### **5. Data Transformation**

Parsed JSON data into structured columns using PySpark transformations.

Applied time-based aggregations for metrics like average temperature and humidity.

Stored data in Delta format for optimized querying.
![Screenshot (102)](https://github.com/user-attachments/assets/d2b9e2b4-785f-4efa-87e3-d2268734aa53)
![Screenshot (103)](https://github.com/user-attachments/assets/39cd7b79-850c-434d-a24f-3b234236ba8f)

### **6. Data Governance and Access Control**

-Managed data schemas and access permissions using Unity Catalog.

-Implemented data validation checks to ensure quality and compliance.
![Screenshot (106)](https://github.com/user-attachments/assets/7a58b72b-499b-4cb8-b955-6bfdcc1863fe)


### **7. Visualization**

Connected Power BI to the Gold Layer in the Delta Lake using DirectQuery.

## **Key Takeaway**

This project highlights my ability to design and implement a production-ready, real-time data pipeline, showcasing skills in scalable data engineering and advanced analytics.
