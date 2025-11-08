# CSC1109 – Data at Speed & Scale (Assignment 1)

**Student:** Luqman Rostam  
**Topic:** Pig and Hive on Hadoop MapReduce  

---

## Overview
This project demonstrates the use of Apache Pig and Apache Hive within the Hadoop ecosystem to clean and analyze university ranking datasets from CWUR and Times Higher Education.

---

## Tasks Completed

### 1. Data Cleaning with Pig
- Script: `clean_universities.pig`
- Loaded raw CSV datasets, removed null and invalid values.
- Saved cleaned data to HDFS under `/user/hive/data/university_rankings_clean/`.

### 2. Simple Analysis with Hive and Pig
- Calculated the average CWUR score per country (`avg_country_score.pig`).
- Calculated the average Times score per year and country (`avg_country_year_score.pig`).
- The same queries were replicated in Hive to verify consistency of results.

### 3. Complex Analysis with Hive
- Used aggregate functions (`MAX` and `MIN`) to compare scores across countries.
- Performed a `JOIN` between CWUR and Times datasets based on university names.
- Executed a `TABLESAMPLE` query for random sampling and grouped analysis.

---

## Repository Structure
| Folder | Description |
|--------|--------------|
| `datasets/` | Contains all raw input CSV files. |
| `pig_scripts/` | Pig scripts for cleaning and analysis. |
| `hive_queries/` | Hive SQL queries for simple and complex analysis. |
| `logs/` | Pig job log files for tracking execution. |
| `screenshots/` | Output screenshots showing successful query results. |

---

## Technologies Used
Apache Hadoop  
Apache Pig  
Apache Hive  
HDFS  
MapReduce  

---

## Summary
The assignment demonstrates a complete end-to-end data processing workflow — from cleaning raw datasets to performing analytical queries using Pig and Hive on Hadoop. It highlights how large-scale data can be processed efficiently using distributed tools.

---

**Date:** November 2025  
**Author:** Luqman Rostam
