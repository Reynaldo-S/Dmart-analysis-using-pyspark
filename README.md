# Dmart Analysis using PySpark

## Project Overview
This project aims to integrate and analyze sales data from three different sources: product information, sales transactions, and customer details using PySpark. The goal is to establish a data pipeline to clean, transform, and analyze the data, answering a series of analytical questions.

### Technologies Used:
- **Python**: Programming language used for data processing.
- **PySpark**: For distributed data processing and analysis.

## Problem Statement:
The task involves creating a data pipeline using PySpark to integrate and analyze sales data from three different sources:
- **Product Information**
- **Sales Transactions**
- **Customer Details**

The project involves loading the data, performing data cleaning and transformation, and answering a set of analytical questions related to sales and customers.

## Tasks and Approach:

### Task 1: Establish PySpark Connection
- Set up the PySpark environment.
- Create a connection to read CSV files into PySpark DataFrames.

### Task 2: Load Data into PySpark DataFrames
- Load the **products.csv**, **sales.csv**, and **customer.csv** files into separate PySpark DataFrames.

### Task 3: Data Transformation and Cleaning
- Perform necessary data cleaning and transformation:
  - Rename columns for consistency if needed.
  - Handle missing values appropriately.
  - Ensure correct data types for each column.
  - Join DataFrames on relevant keys (Product ID and Customer ID).

### Task 4: Data Analysis and Querying
- Formulate **10 analytical questions** and answer them using PySpark:
  1. Total sales for each product category.
  2. Customer with the highest number of purchases.
  3. Average discount across all products.
  4. Unique products sold in each region.
  5. Total profit generated in each state.
  6. Product sub-category with the highest sales.
  7. Average age of customers in each segment.
  8. Orders shipped by each shipping mode.
  9. Total quantity sold in each city.
  10. Customer segment with the highest profit margin.

### Task 5: Run Queries on PySpark
- Implement and run PySpark queries to analyze the data and answer the above questions.

