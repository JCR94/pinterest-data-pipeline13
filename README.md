# Pinterest Data Pipeline

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, we'll create a similar system using a mix of cloud services, DataBricks notebooks, and local scripts.

### Table of contents:
- [Project description](#project-description)
    - [The data](#the-data)
    - [The pipeline](#the-pipeline)
        - [Batch processing](#batch-processing)
        - [Stream processing](#stream-processing)
        - [Source of data: emulating user posting](#source-of-data-emulating-user-posting)
- [Installation requirements](#installation-requirements)
    - [Cloud services](#cloud-services)
    - [Python scripts and notebooks](#python-scripts-and-notebooks)
    - [Modules](#modules)
- [How to use](#how-to-use)
    - [Mounting the S3 bucket to DataBricks](#mounting-the-s3-bucket-to-databricks)
    - [Feeding data to the pipeline](#feeding-data-to-the-pipeline)
    - [Queries on batch-processed data](#queries-on-batch-processed-data)
    - [Reading and writing stream-processed data to Delta Tables](#reading-and-writing-stream-processed-data-to-delta-tables)
- [File structure](#file-structure)
- [Languages and modules used](#languages-and-modules-used)
   - [Languages](#languages)
   - [Modules](#modules-1)
   - [Other technologies used](#other-technologies-used)
- [License information](#license-information)

## Project description

This project is part of the AICore immersive course in data engineering. The goal of this project is to create two parts of a data pipeline, one that deals with batch processing, and one with real-time processing of data streams.

The batch-processing part of the pipeline follows an ELT process, i.e. it

1. extracts data from an infinite loop of generated data, simulating real-time user posting,
2. loads the data into the correct storage location,
3. transforms the loaded data that we can then use to analyze the data.

The real-time processing follows an ETL process, i.e. it

1. extracts data from an infinite loop of generated data, simulating real-time user posting,
2. transforms the streamed data by cleaning it with the same methods we used for batch-processing,
3. loads the clean data into Delta Tables.

### The data

The data comes in the form of three tables:

- Pinterest data, which describes the data from pinterest posts.
- Geolocation data for the post.
- User data for the user who created the pinterest post.

After processing the data into dataframes, the stucture of the different dataframes will look as follows.

- The pinterest data:

    ```bash
    df_pin
    |-- ind: integer (nullable = true)
    |-- unique_id: string (nullable = true)
    |-- title: string (nullable = true)
    |-- description: string (nullable = true)
    |-- follower_count: integer (nullable = true)
    |-- poster_name: string (nullable = true)
    |-- tag_list: string (nullable = true)
    |-- is_image_or_video: string (nullable = true)
    |-- image_src: string (nullable = true)
    |-- save_location: string (nullable = true)
    |-- category: string (nullable = true)
    ```

- The geolocation data:
    
    ```bash
    df_geo
    |-- ind: integer (nullable = true)
    |-- country: string (nullable = true)
    |-- coordinates: array (nullable = false)
    |    |-- element: float (containsNull = true)
    |-- timestamp: timestamp (nullable = true)
    ```

- The user data:

    ```bash
    df_user
    |-- ind: integer (nullable = true)
    |-- user_name: string (nullable = false)
    |-- age: integer (nullable = true)
    |-- date_joined: timestamp (nullable = true)
    ```

### The pipeline

Our pipeline is made out of two main components, one that deals with batch processing, and one that deals with stream processing of our data.

#### Batch processing

- On an EC2 instance, we set up Kafka and connect to an already existing MSK cluster. In that cluster, we create three topics, one for each of the dataframes described above.
- We connect the MSK cluster to an already existing S3 bucket. This bucket will be where we store our batch data.
- We set up and configure an API that sends data to the MSK cluster. Any data that we sent throught the API is now stored in our S3 bucket.
- We set up a Databricks account and mount our bucket to it. The code to mount the bucket is stored in a notebook called `mount-S3-bucket`.
- Using several notebooks in Databricks, we extract the data (`data_extraction`), clean it (`data_cleaning`) and perform a variety of queries on it (`queries`).
- Finally, we set up an Airflow environment to orchestrate our Databricks workload. The DAG can be rewritten to perform any necessary tasks in our pipeline, but by default simply performs all the queries.

#### Stream processing

- We create three Kinesis data streams, one for each table of data.
- We configure the same API we used for batch processing to send data to our data streams as well.
- We read the stream data in Databricks, clean it using the same methods as in batch processing, then store write the data to Delta Tables. This is all done in a notebook called `stream_processing`.

#### Source of data: emulating user posting

For both components, our data originates from an RDS database. We use an infinite loop to select a random row from each table, then send that data to our pipeline via the API we defined. This is supposed to imitate real-time posts made by users at small random intervals.

The loop that sends data to our the batch processing pipeline is written in `user_posting_emulation.py`, the one for stream processing is written in `user_posting_emulation_streaming.py`.

## Installation requirements

### Cloud services

Before starting any of the steps defined in the project description, you will need to set up or have access to

- An MSK cluster to manage your Kafka service.
- An S3 bucket to store batch-processed.
- An MWAA environment to orchestrate your DataBricks workload.
- An S3 bucket to store your DAG in for your MWAA enviroment.

These services and any relevant roles were already provided as part of the project by AICore. Once these services are set up, we set up the remaining services described in the [batch-processing](#batch-processing) and [stream-processing](#stream-processing) sections. Detailed steps are found in the provided AICore notebook lessons.

### Python scripts and notebooks

The following two Python scripts are needed to upload batch- and streaming data, respectively:
- `user_posting_emulation.py`
- `user_posting_emulation_streaming.py`

They can be found in the repository. A version of `user_posting_emulation.py` was also provided by AICore as a starting point.

The following notebooks in the repository have to be imported to your DataBricks workspace.
- `data_cleaning.ipynb`
- `data_extraction.ipynb`
- `mount-S3-bucket.ipynb`
- `queries.ipynb`
- `stream_processing.ipynb`

### Modules

The following 3rd party libraries have to be installed locally:

- Airflow
- Requests
- Boto3
- json
- SQLAlchemy
- Pymysql

### Files

```bash
.
├── python_scripts
│   ├── 0a6a638f5991_dag.py
│   ├── user_posting_emulation.py
│   └── user_posting_emulation_streaming.py
├── notebooks
│   ├── data_cleaning.ipynb
│   ├── data_extraction.ipynb
│   ├── mount-S3-bucket.ipynb
│   ├── queries.ipynb
│   └── stream_processing.ipynb
├── .gitignore
└── README.md
```

## How to use

### Mounting the S3 bucket to DataBricks

To mount the S3 bucket to DataBricks, simply run the `mount-S3-bucket` notebook after adjusting the existing bucket name and its mount name in DataBricks.

In one of the cells, you will find the following two lines defining the `ACCESS_KEY` and `SECRET_KEY` that are extracted from a `credentials.csv` file, required to authorize acces to the bucket:

```python
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
```

However, if you run into an `InvalidMountException` or `RemoteException`, it might be advised to store the credentials in a separate `.json` or `.yaml` file and read them from there. Alternatively, you may hard-code the credentials into the cell, though for privacy reasons this is not advised.

The last cell contains a line to unmount the bucket.

```python
dbutils.fs.unmount("/mnt/MOUNT_NAME")
```

The line is commented out so it doesn't run by default when running the entire notebook. It can be useful to unmount the bucket to restart the mounting process, e.g. if you run into problems with your credentials as described above.

### Feeding data to the pipeline

Connect to your EC2 client via SSH. You can do so by defining the permissions on you key file `key-pair.pem` first
```bash
chmod 400 key-pair.pem
```
then exectuing the following line with the right parameters
```bash
ssh -i key-pair.pem ec2-user@public_dns_name
```
A tutorial is also provided in your EC2 console.

Then, start the Kafka client inside your EC2 instance. In our filesystem, we do so with the following line.
```bash
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
```

Now your API is ready to be used and you can feed data through your API. For batch processing you do so by running

```bash
python user_posting_emulation.py
```

This will enter an infinite loop that constantly sends data to your MSK cluster (which then stored in your S3 bucket). Whereas for stream processing, you use

```bash
python user_posting_emulation_streaming.py
```
which feeds the data to your Kinesis stream.

### Queries on batch-processed data

The `queries` notebook includes a variety of pre-defined queries. They are written in Python, so they can be seamlessly integrated into any Python code, but the SQL can still be extracted from it. To run the queries, simply run the notebook or any individual cell. If you choose to run individual queries, the first five cells still have to be run first, regardless. The first cell simply imports the necessary modules:

```python
import pyspark
import multiprocessing
```

The second cell runs the `data_cleaning` notebook so that the cleaned data can be imported for the queries:

```python
%run /path/to/data_cleaning
```

The third cell loads the cleaned data into three dataframes:

```python
df_pin, df_geo, df_user = load_cleaned_data()
```

The fourth cell creates the session to run queries
```python
cfg =(
    pyspark.SparkConf()
    ...
)

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
```

Finally, the fifth cell creates the TempViews for each table:

```python
df_pin.createOrReplaceTempView("pin_table")
df_geo.createOrReplaceTempView("geo_table")
df_user.createOrReplaceTempView("user_table")
```

Now any other cell can be run without issue to run the corresponding query. To display any table simply use the display method

```python
display(query_table_name)
```

### Reading and writing stream-processed data to Delta Tables

To read the data from the Kinesis data streams, as well as to write the incremental data to Delta Tables in DataBricks, we use the `stream_processing` notebook.

As with batch processing, we run the first two cells to import the necessary modules and have access to our data-cleaning methods.

The third cell gives us our `ACCESS_KEY` and `SECRET_KEY` that are necessary to read data from the stream. As described in the [first step of this section](#mounting-the-s3-bucket-to-databricks), you may encounter some exceptions, in which case you may have to rewrite the cell to access the credentials differently.

The fourth cell defines a method to read a kinesis data stream into a dataframe, as well as a method that converts said dataframe into a dataframe with a given schema.

``` python
def read_kinesis_stream(stream_name):
    ...

def convert_kinesis_stream_to_dataframe(stream_name, streaming_schema):
    ...
```

The next cells define the appropriate schema for each table of raw data, then load the streamed data using the correct schema. We can then simply apply our cleaning methods (the same that we used for batch-processing data above). The cleaned dataframes can be displayed in the usual way, e.g.

```python
display(df_pin_clean)
```

Using the `display` method will make the cell run infinitely and keep displaying incoming data until interrupted.

Finally, to write the streamed, cleaned data into Delta Tables, we apply the following method to each dataframe:

```python
def write_df_to_delta_table(df, table_name):
    ...
```

Similarly to displaying data, this method will run infinitely and keep writing incoming data until interrupted.

## File structure

```bash
.
├── python_scripts
│   ├── 0a6a638f5991_dag.py
│   ├── user_posting_emulation.py
│   └── user_posting_emulation_streaming.py
├── notebooks
│   ├── data_cleaning.ipynb
│   ├── data_extraction.ipynb
│   ├── mount-S3-bucket.ipynb
│   ├── queries.ipynb
│   └── stream_processing.ipynb
├── .gitignore
└── README.md
```

## Languages and modules used

### Languages
- Python
- SQL (Postgres)
- Bash

### Modules

- DateTime
- Multiprocessing
- Random
- Time
- Airflow
- Boto3
- json
- PyMySQL
- PySpark
- Requests
- SQLAlchemy
- URLLib

### Other Technologies used

- AWS
    - EC2
    - IAM
    - MSK
    - S3
    - API Gateway
    - MWAA
    - Kinesis
- Kafka
- Databricks

## License Information

TBD