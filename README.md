Netflix Data Analysis with Amazon S3 and Athena

This project demonstrates how to analyze a Netflix dataset using AWS serverless analytics tools. The dataset is stored in Amazon S3, and Amazon Athena is used to query the data directly using SQL. The goal is to extract insights about Netflix content such as movies, TV shows, release trends, ratings, and genres.
This project shows how cloud-based data analytics can be performed without managing servers or infrastructure.

Project Overview:
Netflix offers thousands of movies and TV shows across many countries and genres. In this project, we analyze a Netflix dataset to understand patterns such as:
 Distribution of Movies vs TV Shows
 Content release trends over the years
 Top countries producing Netflix content
 Most common ratings and genres
 The workflow involves storing data in Amazon S3 and analyzing it using Amazon Athena SQL queries.

Architecture:
 Netflix Dataset → Amazon S3 → Amazon Athena → SQL Queries → Insights
 Upload dataset to Amazon S3
 Connect Amazon Athena to the S3 bucket
 Create table schema in Athena
 Run SQL queries for analysis

Technologies Used:
 Amazon S3 – Data storage
 Amazon Athena – Serverless SQL query engine
 SQL – Data analysis
 CSV Dataset – Netflix titles dataset

Project Structure:
Netflix-Data-Analysis-with-S3-and-Athena
│
├── dataset
│   └── netflix_titles.csv
│
├── queries
│   └── netflix_analysis_queries.sql
│
├── screenshots
│   └── athena_results.png
│
└── README.md

Dataset
The dataset contains information about Netflix titles including:
 Column	Description
 show_id	Unique identifier
 type	Movie or TV Show
 title	Title of the content
 director	Director name
 cast	Actors in the show
 country	Country of production
 date_added	Date added to Netflix
 release_year	Year of release
 rating	Content rating
 duration	Duration or number of seasons
 listed_in	Genre/category
 description	Short summary

Setup Instructions
1. Upload Dataset to Amazon S3
 Go to AWS Management Console
 Open Amazon S3
 Create a new bucket
 Upload the dataset file netflix_titles.csv
2. Create Database in Athena
 CREATE DATABASE netflix_db;
3. Create Table in Athena
 CREATE EXTERNAL TABLE netflix_titles (
    show_id STRING,
    type STRING,
    title STRING,
    director STRING,
    cast STRING,
    country STRING,
    date_added STRING,
    release_year INT,
    rating STRING,
    duration STRING,
    listed_in STRING,
    description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
LOCATION 'locationname';

 Author
RITHIKA

