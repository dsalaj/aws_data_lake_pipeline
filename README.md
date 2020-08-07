# AWS pyspark data pipeline for trends analysis in news and social media

AWS data pipeline (S3, EMR, Redshift) for finding the trends in social networks and news

## Scope

The target usecase is to provide a convenient way of detecting and analyzing trends of Named Entities in the news and social media. This is implemented by the data pipeline which performs Named Entity Recognition as a part of the ETL process. The pipeline can ingest various sources of data and fills the star schema postgreSQL database which enables easy analysis queries.

## Data Sources

The data sources:

- Reddit submissions [link](http://files.pushshift.io/reddit/submissions/)
- All the news dataset [link](https://components.one/datasets/all-the-news-2-news-articles-dataset/)

The choice of datasets is arbitrary. The pipeline should be easily adaptable to any type of dataset by modifying the `scripts/merge_data.py`. The fields of interest are the titles (of news articles of reddit submissions, but the NLP will work on longer text content also) and the timestamps.



## Data Model

The star schema database model was chosen to make the analytics queries simpler and more efficient.
With star schema the join statements are simpler and provide performance enhancements for read-only analytical and reporting queries. It is chosen to easily support OLAP queries.

### Fact table


| fact_news  |
|------------|
|title_id    |
|date_id     |
|ner_id      |

### Dimensions tables

| dim_title  |
|------------|
|id          |
|title       |

| dim_ner    |
|------------|
|id          |
|text        |
|label       |

| dim_date   |
|------------|
|id          |
|date        |


## Technology Stack

A local Airflow instance is deployed using:

    docker-compose -f docker-compose-CeleryExecutor.yml up

This Airflow instance then orchestrates the deployment and execution of steps on the AWS cloud.
These include:

- Start
- Uploading pyspark scripts for ETL to S3
- Creating EMR cluster
- Executing ETL steps on EMR cluster
  - Cleaning and merging datasets to a single parquet file
  - Named Entity Recognition
  - Transforming the data to star schema
- Creating Redshift cluster
- Loafing the prepared data from S3 to Redshift

The ETL steps are implemented using `pyspark` which provides a scalable way of performing transformations and processing of large data in a parallel manner.

The NLP library is `spacy` as it excels at large-scale information extraction tasks and is popular in the industry.

## Getting started

First build the Airflow docker image:

    sudo docker build --rm --build-arg AIRFLOW_DEPS="aws" --build-arg PYTHON_DEPS="boto3" -t puckel/docker-airflow .

Next start the Airflow:

    sudo docker-compose -f docker-compose-CeleryExecutor.yml up -d

## Scenario questions

How you would approach the problem differently under the following scenarios:

- If the data was increased by 100x?

The bottleneck of the pipeline is the Named Entity Recognition executed as a part of the ETL process. This requires a significant amount of computation and a more efficient library for NER would increase the speed and reduce the costs of this step.
Another improvement would be possible in the preprocessing step where a heuristic could be designed to exclude some titles that are not of interest for the specific usecase. This would drastically reduce the cost and time of pipeline execution. 

- If the pipelines were run on a daily basis by 7am?

This can be supported by modifying the `schedule_interval` parameter of the DAG to a daily execution, and setting the time of the `start_date` to 7am.

- If the database needed to be accessed by 100+ people?

The star schema postgres database provides sufficient reading bandwidth to support the usage of 100+ people.