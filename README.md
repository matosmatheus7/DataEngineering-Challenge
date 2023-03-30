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

I have observed that when manually converting pivot cache data to upload to **Google Colab**, the data can be inconsistent, resulting in mismatched volume values for each month. In such cases, it is necessary to rotate the date to ensure that the volumes values from each month match accurately. However, by using the **openpyxl** library to extract the pivot cache on Airflow, the data is correctly processed without requiring any additional steps, and the column unit is already created, thereby saving time and effort.

To ensure consistency across environments, I used **Docker** to create an extended image that allowed me to run the same version of **Python** and **Pandas** that I used in Google Colab. This ensured that the solution was reliable and reproducible across different systems.
### Folder Structure
```bash
airflow
├── dags
│   └── pipeline.py
├── staging
│   ├── vendas_diesel_m3.xlxs
│   └── vendas_dfuels_m3.xlxs
│   └── vendas-combustiveis-m3.xlsx
├── assests
│   ├── image1.png
│   └── image2.png
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
    
The DAG (Directed Acyclic Graph) is designed to run once a month. The extraction process involves **downloading** the latest file from the official ANP website, which we then save to the staging folder. We use the **openpyxl** library to extract the pivot cache from the download .xlsx file, storing the results in two separate .xlsx tables also located in the staging folder.

It's worth noting that the download process will only occur if the file doesn't already exist in the staging folder or if the existing file is older than 30 days (1 month). This ensures that we're always working with the most up-to-date data. In the event that an old .xlsx file exists, it will be replaced with the new file.

### Transformation 
   
My primary task in the ETL process is to perform a series of transformations on the data to meet the specific requirements of the challenge. The first step is to **unpivot the months columns**, which involves converting the data from a wide format to a long format. This makes it easier to analyze and manipulate the data.
In addition to the unpivot operation, there may be other minor operations required to adapt the data to meet the challenge requirements. For example, I may need to perform data type conversions, remove or rename columns, or handle missing or null values.

During these transformation steps, I also create a **validation task**, to ensure data consistency by checking the total value from the pivote tables that were extracted with the values from the transformed data frames. If data is not valid the data is not loaded.
By performing these transformations and validations, we can ensure that the data is in the correct format and meets the requirements of the challenge.

### Load
The final step in the ETL process is to load the transformed data into a Postgres table. This involves inserting both .xlsx files that were generated during the transformation process into the **anp_sales** table in **Postgres**.

The **report task**, has the objective to inform that the pipeline has been complete. It will improve communication and keep the pipeline owner informed about the pipeline's progress.

By completing this step, the data is available for further analysis, in a format that is easy to work with and meets the specific requirements of the challenge.

![N|Solid](https://github.com/matosmatheus7/DataEngineering-Challenge/blob/main/assets/postgree_snapshot.PNG?raw=true)

## :point_up: Improvement Points
- Consider using PySpark to process data, I decided to go with pandas because dataset is small, making Pandas and beter solution than Pyspark, also PySpark environments are more complex to set up, but pyspark is optimized for large-scale data processing and can reduce the time cost compared to traditional Python data processing methods.
-   Instead of just printing a message using `printf`, send an email with the status of the pipeline to the pipeline owner for the reporting task. This will improve communication and keep the pipeline owner informed about the pipeline's progress.
-   Explore with the stakeholdes the option to replace null values with the mean of the corresponding month/year/uf. This approach will help fill in missing data and make the data analysis more accurate.

### Clone project

    $ git clone https://github.com/matosmatheus7/DataEngineering-Challenge

### Build airflow Docker

    $ cd DataEngineering-Challenge
    $ docker build . --tag extending_airflow:latest
  
### Launch containers
    $ docker-compose up 
## :notebook:Useful links
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

- https://pandas.pydata.org/docs/reference/api/pandas.melt.html

- https://stackoverflow.com/questions/37354105/find-the-end-of-the-month-of-a-pandas-dataframe-series

- https://www.statology.org/pandas-unpivot/#:~:text=In%20pandas%2C%20you%20can%20use,col3'%2C%20...%5D

- https://ask.libreoffice.org/t/convert-to-command-line-parameter/840

- https://www.w3schools.com/python/ref_keyword_assert.asp

# :crossed_swords: Thanks for the Opportunity !
