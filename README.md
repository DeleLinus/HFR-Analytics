## Table of Contents
1. [Project Description](#desc)
2. [About Nigeria HFR](#about)
3. [ Project Development](#dcue)
4. [ References ](#ref)


<a name="desc"></a>
# Prototype End-to-end Analytics Solution on Cloud for the Nigeria Health Facility Registry (HFR)

This is an end-to-end data engineering project that entails the design and implementation of a data warehouse for the Nigeria Health Facility Registry which is itself a program under the Nigeria Ministry of Health.

This  project development emcompasses:
1. The design of a **data ingestion pipeline architecture** showing the different Google Cloud Serevices, tools and framework used.
2. The design of the **ERD(Entity Relationship Diagram)** of the
system and schema of my final data mart used for reporting.
3. **The implementation of Data Extraction, Transformation and Loading (ETL)** with **Google Cloud Integration**
4. The **Automation and Scheduling** of the processes
5. Basic analytics
 

<a name='about'></a>
# About Nigeria HFR
The Nigeria Health Facility Registry (HFR) was developed in 2017 as part of effort to dynamically manage the Master Health Facility List (MFL) in the country. The MFL "is a complete listing of health facilities in a country (both public and private) and is comprised of a set of identification items for each facility (signature domain) and basic information on the service capacity of each facility (service domain)".

The Federal Ministry of Health had previously identified the need for an information system to manage the MFL in light of different shortcomings encountered in maintaining an up-to-date paper based MFL. The benefits of the HFR are numerous including serving as the hub for connecting different information systems thereby enabling integration and interoperability, eliminating duplication of health facility lists and for planning the establishment of new health facilities.


<a name='dcue'></a>
# Project Development

## Data Ingestion Pipeline Architecture
This design has been made using  https://www.app.diagrams.net/ .

![on_cloud drawio](https://github.com/DeleLinus/HFR-Analytics/assets/58152694/46c03436-5de6-4178-a3b5-a9486d69aa68)


* The data source as provided remains the [HFR website](https://bit.ly/3lVu5C6) 
* The Data Extraction shall be carried out by utilizing the **Selenium Python web automation framework**  
* Data staging on **Google Cloud Storage (GCS)**
* Data Transformation using DataProc- **PySpark** 
* The workflow management or orchestration tool of choice for the Scheduling and Automation is the **Cloud Composer**
* And the **Google Big Query (GBQ)** used for the final data warehouse while the **Google Colaboratory** with python has been used to analyse and visualize the data for answers

## ERD(Entity Relationship Diagram) Design
The ERD  of the system as shown below can also be accessed [here](https://lucid.app/lucidchart/3b297f6f-6dd4-40b4-805c-263f42043573/edit?viewport_loc=50%2C288%2C2560%2C1052%2C0_0&invitationId=inv_c95a73c5-49bf-464c-845e-37e7e9b6ba7e#)

![Edited DBMS ER diagram (UML notation)](https://user-images.githubusercontent.com/58152694/183575858-3943cafc-f0db-4bc8-b1ee-bba3d6ce8808.png)

From considering the HFR requirements and studying the value types and forms of data available on the HFR website:
* I have designed a **Star schema** as my final data mart showing six (6) dimension-tables.Having performed 3 levels of normalization (1NF, 2NF, 3NF) where applicable.
* The model has also been designed to provide information for all possible grains. i.e the fact table rows  provide a high level of details.
* This stage I would say has been the most tasking.
  
## Data Extraction, Transformation and Loading (ETL) Implementaion
* The python frameworks and packages leveraged for the web scraping are **Selenium**, **Pandas**, **Numpy** and **BeautifulSoup**.
* To speed up the webscraping process, **multithreading** has been employed
* Output of the scraper is saved as `raw_hfr_data.csv` while being partitioned by dates in the cloud storage. 
* **Spark** has been largely utilized for the whole **ETL** process
* A **doctors.parquet** file was written (i.e data of health institution that has at least one doctor)
* **Data Loading** into **GBQ** has been performed as proposed in the ERD and schema design 

## Automation and Scheduling
Two separate DAGS were written, one for the scraper to run daily and the other to perform ingestion, transfommation and staging, and loading into the GBQ.

![Dag running](https://github.com/DeleLinus/HFR-Analytics/assets/58152694/7c80dbf6-2e3b-4618-b616-9e14f6e87488)
![DAG state](https://github.com/DeleLinus/HFR-Analytics/assets/58152694/c42d2365-422f-4b43-84c8-71ec74b94bcd)
![DAG success](https://github.com/DeleLinus/HFR-Analytics/assets/58152694/ccae87f5-ca56-46cf-9595-2e2d238bc5b2)


## Analytics
The Data Warehouse was Queried as shown below to get names of institutions/health facilities and the number of Doctors present:
![DWH_new](https://github.com/DeleLinus/HFR-Analytics/assets/58152694/b5e411f3-9b57-4ee6-b913-0ceb1e8a71f6)

and then exported to Colab for visualization and basic descriptive analysis. The plot below shows the "top ten institutions with the highest number of doctors":


![top institutions](https://github.com/DeleLinus/HFR-Analytics/assets/58152694/a4de5a6f-ed4d-4e8c-af05-f918be439309)


<a name="ref"></a>
## References

- [About HFR](https://hfr.health.gov.ng/about-us)
- [Architecture Design](https://www.app.diagrams.net/)
- [prisma](https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database)
- [Google Cloud Skills Boost](https://www.cloudskillsboost.google/)
