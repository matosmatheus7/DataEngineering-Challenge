# :fuelpump: DataEngineering-Challenge
#### ANP Fuel Sales Data Challenge
This test consists in developing an ETL pipeline to extract internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP (National Agency for Petroleum, Natural Gas and Biofuels).

### Requirements
- Airflow
- Infrastructure as Code
- The totals of the extracted data must be equal to the totals of the pivot tables.
- Data should be stored in the following format:

    | Column       | Type        |
    | ------------ | ----------- |
    | `year_month` | `date`      |
    | `uf`         | `string`    |
    | `product`    | `string`    |
    | `unit`       | `string`    |
    | `volume`     | `double`    |
    | `created_at` | `timestamp` |

# :bulb: Solution
In order to develop an MVP for the ANP fuel sales ETL process, I initially used **Google Colab** to explore and investigate the data due to its user-friendly interface and fast deployment capabilities.

Then I start implementing the **Airflow**, in order to orchestrate the ETL process. This involved downloading sales data for oil derivative fuels and diesel from the government website, extracting the pivot cache, unpivoting the table, validating totals, and inserting all the data into a **PostgreSQL** table in the requested format.

I have observed that when manually converting pivot cache data on **Google Colab**, the data can be inconsistent, resulting in mismatched volume values for each month. In such cases, it is necessary to rotate the date to ensure that the volumes values from each month match accurately. However, by using the **openpyxl** library, the data is correctly processed without requiring any additional steps, and the column unit is already created, thereby saving time and effort.

To ensure consistency across environments, I used **Docker** to create an extended image that allowed me to run the same version of **Python** and **Pandas** that I used in Google Colab. This ensured that the solution was reliable and reproducible across different systems.
### Folder Structure
```bash
airflow
├── dags
│   └── pipeline.py
├── staging
│   ├── vendas_diesel_m3.xlxs
│   └── vendas_dfuels_m3.xlxs
├── assests
│   ├── image1
│   └── image2
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── README.md
├── .env
└── .gitignore
└── anp_data_exploring.ipynb
```
### ETL Tasks
![N|Solid](https://github.com/matosmatheus7/DataEngineering-Challenge/blob/main/assets/dag_snapshot.PNG?raw=true)

### Extraction
This DAG will run every month on the first day of the month. The extraction process involves downloading the latest file from the **official ANP website** and saving it to the staging folder. Using the **openpyxl** library, we load the pivot cache from the requested tables and store the data in two separate .xlsx tables in the staging folder.


### Transformation 
My primary task in the ETL process is to perform a series of transformations on the data to meet the specific requirements of the challenge. The first step is to **unpivot the months columns**, which involves converting the data from a wide format to a long format. This makes it easier to analyze and manipulate the data.
In addition to the unpivot operation, there may be other minor operations required to adapt the data to meet the challenge requirements. For example, I may need to perform data type conversions, remove or rename columns, or handle missing or null values.
During these transformation steps, I also create a **validation task**, to ensure data consistency by checking the total value from the pivote tables that were extracted with the values from the transformed data frames.
By performing these transformations and validations, we can ensure that the data is in the correct format and meets the requirements of the challenge.

### Load
The final step in the ETL process is to load the transformed data into a Postgres table. This involves inserting both .xlsx files that were generated during the transformation process into the **anp_sales** table in **Postgres**.
By completing this step, the data is available for further analysis, in a format that is easy to work with and meets the specific requirements of the challenge.

![N|Solid](https://github.com/matosmatheus7/DataEngineering-Challenge/blob/main/assets/postgree_snapshot.PNG?raw=true)

## :point_up: Improvement Points
- Consider using PySpark to process data, as it is optimized for large-scale data processing and can reduce the time cost compared to traditional Python data processing methods.
~~- Check if the file exists and its modification date. If the file already exists and it was downloaded less than one month ago, skip downloading a new file. This can reduce unnecessary file downloads and improve performance.~~ :heavy_check_mark:
-   Instead of just printing a message using `printf`, send an email with the status of the pipeline to the pipeline owner for the reporting task. This will improve communication and keep the pipeline owner informed about the pipeline's progress.
-   Explore with the stakeholdes the option to replace null values with the mean of the corresponding month/year/uf. This approach will help fill in missing data and make the data analysis more accurate.
- Since I choose to insert both dataframes on 1 table, I needed t use the **if_exists='append'** attribute when inserting on the database. Now, if I don't want to change my database, I should look for an option to check for duplicate rows based on year-month and remove it.
## :hammer_and_wrench: Setup

### Clone project

    $ git clone https://github.com/matosmatheus7/DataEngineering-Challenge

### Build airflow Docker

    $ cd DataEngineering-Challenge
    $ docker build . --tag extending_airflow:latest
  
### Launch containers
    $ docker-compose up 
## :notebook:Useful links
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html

- https://pandas.pydata.org/docs/reference/api/pandas.melt.html

- https://stackoverflow.com/questions/37354105/find-the-end-of-the-month-of-a-pandas-dataframe-series

- https://www.geeksforgeeks.org/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/

- https://www.w3schools.com/python/ref_keyword_assert.asp

- https://www.w3schools.com/python/python_try_except.asp
# :crossed_swords: Thanks for the Opportunity !


