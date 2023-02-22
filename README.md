# Traffic Accidents on BRs

This project implements a simple data lakehouse architecture using the medallion pattern to incrementally process traffic accident data and then serve to a Dashboard.

Here’s a summary of the process:

- The data is extracted from the source with a python script
- A Data Pipeline built with PySpark populates the Medallion Architecture Layers
    - 🥉Bronze (local): The raw CSV files are joined in a stage Delta Lake to normalize the columns.
    - 🥈Silver (local): Standardize numbers and date formats, and normalize text fields.
    - 🥇Gold (GCP Bucket): Aggregates the data to reduce size, complexity, and granularity.
- A Dashboard built with Streamlit reads the Gold Layer data and displays the charts. [Access the Dashboard here](https://jaumpedro214-traffic-accidents-br-data-pro-dashboardmain-0w6njh.streamlit.app/)

 Stack & Tools: <img src="https://img.shields.io/static/v1?label= &message=Python&color=3776AB&style=flat&logo=PYTHON&logoColor=white"/>
        <img src="https://img.shields.io/static/v1?label=&message=Spark&color=E25A1C&style=flat&logo=APACHESPARK&logoColor=white"/>
        <img src="https://img.shields.io/static/v1?label=&message=Delta Lake&color=00ADD4&style=flat"/>
        <img src="https://img.shields.io/static/v1?label= &message=Streamlit&color=fc4c4c&style=flat&logo=STREAMLIT&logoColor=white"/>
        <img src="https://img.shields.io/static/v1?label= GCP&message=Bucket&color=FFFFFF&style=flat&logo=GOOGLECLOUD&logoColor=white"/>
        <img src="https://img.shields.io/static/v1?label= &message=Docker&color=2496ed&style=flat&logo=DOCKER&logoColor=white"/>

## Repository organization

All the code was developed to run inside docker containers.

The 📁/src folder contains all pyspark scripts to execute the data pipeline.

The 📁/dashboard folder contains the Streamlit app’s python scripts.

The dashboard and the pipeline are totally independent, in terms of execution, of each other.

## How to run this project

### Running the Pipeline

**(1)** Execute the prepare_environment.sh script

```sql
./prepare_environment.sh
```

It will create the needed folders with the proper authorizations to run the scripts.

**(2)** Install the requirements.txt

```sql
pip install -r requirements.txt
```

**(3)** Put the GCP JSON credentials (to access the bucket) in the /src/credentials folder and the Bucket name in the bucket_name.txt file created.

**(4)** Start the Spark containers.

```sql
docker-compose up
```

Two containers spark containers (master and worker) will be started. 

**(5)** Download the data python `python download_files.py <year>` (currently only 2007 to 2022 are available)

**(6)** Access the internal terminal of a spark container and cd into /src folder

**(7)** Each python script is a spark job. To execute any of them, just type `spark-submit --packages io.delta:delta-core_2.12:2.1.0 <path_to_job_file.py>`

Run the scripts in the following order: merge_raw_files_in_stage.py, raw_to_silver.py, silver_to_gold.py

## Running the Dashboard

Just start the containers with `docker-compose up` and access the browser on localhost:8501
