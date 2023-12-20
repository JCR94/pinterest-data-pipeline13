# Pinterest Data Pipeline

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, we'll create a similar system using the AWS Cloud.

### Table of contents:
- [Project description](#project-description)
- [Installation requirements](#installation-requirements)
   - [Modules](#modules)
   - [Files](#files)
- [How to use](#how-to-use)
	- [Alternative ''master'' main file](#alternative-master-main-file)
- [File structure](#file-structure)
- [Languages and modules used](#languages-and-modules-used)
   - [Languages](#languages)
   - [Modules](#modules-1)
- [License information](#license-information)

## Progress

1. First we set up an empty repository and AWS account.

2.  Then we downloaded the Pinterest infrastructure. For that, we use the file `user_posting_emulation.py` that contains the login credentials for an RDS database. The RDS database has three tables

    - `pinterest_data`
    - `geolocation_data`
    - `user_data`

    The script in `user_posting_emulation.py` loops infinitely. Ever loop, it fetches a random entry from each table (each entry sitting in the same row of their respective tables).

3. We configured the Kafka EC2 client

    - First, we created a local key-pair file (ending in a `.pem` extension) for an already existing EC2 client in our AWS account. This allows us to connect to our EC2 instance.
    
    - To do so, we once define reading permissions to the owner on the key-pair file:
        ```bash
        chmod 400 key-pair-file.pem
        ```
    - Now we can connect to the EC2 instance through our terminal with the command below. The 
        ```bash
        ssh -i "key-pair-file.pem" ec2-user@<public-dns-name>
        ```
        
        **Note**: if you're working through WSL an Windows, when you try to connect to the SSH client, you may encounter a `"permission too open"`error. Defining the permissionsas described above may not be sufficient.    
    To connect via an SSH client, permissions have to be kept to a minimum, more specifically, keys must only be accessible to the user they're intended for. In WSL, you run as a different user as on Windows (we are still running in a VM, after all). The permissions of your files in your Windows filesystem (anything inside `/mnt/c` relative to WSL) are still under control of Windows. Thus you cannot modify permissions on Windows files in `/mnt/c` from Linux (WSL). Which means you cannot make those permissions exclusive to the user you're running as in WSL.  
    A quick and easy (and recommended) fix is to move your `key-pair-file.pem` file to the `~/.ssh` folder (where `~` refers to the home path of your Linux subsystem). This moves it to a location in the Linux system that doesn't overlap with Windows. From there, you can give the file the necessary permissions as described above and connect to the EC2 instance.

    - We installed Kafka on our EC2 machine (version `2.12-2.8.1`, which is the same version the MSK cluster is running one).

    - We installed the `IAM MSK authentication package` on the EC2 machine, which allows us to connect to IAM authenticated MSK clusters

    - We configured our Kafka client to use IAM authentication to the cluster by modifying the `client.properties` inside our `kafka_folder/bin` directory.

4. We created three Kafka topics, namely
    - `user-id.pin` for the data of the Pinterest post
    - `user-id.geo` for the geolocation data of the post
    - `user-id.user` for the user data of the post

5. We created a custom plugin with MSK Connect by
    - creating an S3 bucket
    - download the `Confluent.io Amazon S3 Connector` on our EC2 client and copy it to the S3 bucket.
    - Create a custom plugin in the MSK Connect console.

6. We created a connector with MSK connect. With the plugin-connector pair, data passing through the MSK cluster will automatically be stored in the S3 bucket.

7. We now configure the API in API Gateway. We start by Building a Kafka REST proxy integration method for the API (the API is already given)

8. We deploy the API under the stage name 'test-stage'.

9. We set up the Kafka REST proxy on the EC2 client.
    - First we install the Confluent package for the Kafka REST Proxy on our EC2 machine.
    - Then we allow the REST proxy to perform IAM authentication to the MSK cluster by modifying the kafka-rest.properties file.
    - Finally, we start the REST proxy on the EC2 client machine.

10. We send some data to the API, which will in turn send it to the MSK cluster.
    - Modify the `user_posting_emulation.py` script to send the data to the Kafka topics using the API invoke URL. We send data for each table.
    - In order to do so, we had to create a serializer method `json_serial`, as dates stored as a `datetime.datetime`-type were not serializable.
    - We succesfully sent data to the cluster, and we can see it being stored in the S3 bucket.

11. We set up the Databricks account. The Databricks account has already been granted access to the S3 bucket, and the credentials have been uploaded to Databricks as `authentication_credentials.csv`

12. We mounted the S3 bucket to Databricks, and created three dataframes: `df_pin`, `df_geo`, and `df_user`, one for each table.

13. Saved code used on Databricks to file `databricks-scripts.ipynb` notebook.

14. We refactored some of the databricks code to store all of the different code in methods that can.

15. We performed a variety of queries on the dataframes.

## Project description

This project is part of the AICore immersive course in data engineering.

Under construction ...

## Installation requirements

### Modules

### Files

```bash
.
├── python_scripts
│   └── user_posting_emulation.py
├── __main__.py
├── .gitignore
└── README.md
```

## How to use

Under construction ...

### Alternative ''master'' main file

Under construction ...

## Languages and modules used

### Languages
- Python
- Bash

### Modules

requests
time
random
multiprocessing
boto3
json
sqlalchemy

### Other Technologies used

- AWS
    - EC2
    - IAM
    - MSK
    - S3
    - API Gateway
- Kafka
- Apache Zookeeper
- Databricls

## License Information

TBD