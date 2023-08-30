# Spotify Global Top 50 Songs ETL Pipeline

This ETL (Extract, Transform, Load) pipeline is designed to fetch the global Top 50 songs from Spotify using the Spotify API. The fetched data is then transformed and cleaned to create four distinct tables: `songs`, `singers`, `composers`, and `albums`. The final step involves loading these tables into an Amazon S3 bucket and performing analytics using AWS Athena.

## Overview

This pipeline automates the process of gathering the global Top 50 songs from Spotify, organizing them into structured tables, and making the data available for analysis on AWS Athena. The main steps of the pipeline are as follows:

1. **Extraction**: Data is fetched using the Spotify API to retrieve the current global Top 50 songs.

2. **Transformation**: The fetched data is cleaned, processed, and organized into four distinct tables:
    - `songs`: Contains information about the songs, such as title and release date.
    - `singers`: Includes details about the singers or artists performing each song.
    - `composers`: Provides information about the composers of the songs.
    - `albums`: Contains details about the albums to which the songs belong.

3. **Loading**: The transformed tables are loaded into an Amazon S3 bucket, making the data accessible for further analysis.

4. **Analytics with AWS Athena**: AWS Athena is used to perform SQL-like queries on the data stored in the S3 bucket. This enables users to gain insights and perform various analyses on the global Top 50 songs dataset.


```
