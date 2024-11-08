
# ELT Project: Layoffs Data Analysis

## Overview

This project demonstrates an ETL (Extract, Transform, Load) process using layoffs data. The primary goal is to extract data from a CSV file, load it into a MySQL database, and perform data cleaning and transformation to make the data ready for analysis.

## Project Structure

- Data Extraction: Extracted data from layoffs.csv.
- Data Loading: Loaded data into a MySQL database for easy querying.
- Data Transformation: Performed data wrangling and cleaning to improve data quality and prepare it for analysis.

## Process

    1. Extraction:
        The data was sourced from a CSV file named layoffs.csv.

    2. Loading:
        Data was imported into a MySQL database using appropriate data types and constraints.

    3. Transformation (Data Cleaning)
       Key data cleaning steps included:

     - Removing Duplicates: Checked for and removed duplicate    rows  to ensure data uniqueness.
     - Standardizing Data: Ensured consistency in data formats and fixed any obvious errors.
     - Handling Null Values: Assessed and handled missing values.
     - Removing Irrelevant Columns/Rows: Filtered out data not  required for analysis.

## Database Setup

- MySQL database was set up to store the cleaned data.
- A table was created with optimized schema to store the  transformed data for easy access and further analysis.     

