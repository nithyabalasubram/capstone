# capstone
Capstone project
# Per Scholas Capstone
#### Final project for the Per Scholas Data Engineering course

## Project requirements:

## Credit Card Dataset Overview
The Credit Card System database is an independent system developed for managing activities
such as registering new customers and approving or canceling requests, etc., using the
architecture.

A credit card is issued to users to enact the payment system. It allows the cardholder to access
financial services in exchange for the holder's promise to pay for them later. Below are three
files that contain the customer’s transaction information and inventories in the credit card
information.

a) CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.

b) CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.

c) CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this
file.

### Business Requirements - ETL

## 1. Functional Requirements - Load Credit Card Database (SQL)

### Req-1.1 Data Extraction and Transformation with Python and PySpark




### 1. Load Credit Card Database (SQL)
#### 1.1) Data Extraction and Transformation with Python and PySpark
a) For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.
1. CDW_SAPP_BRANCH.JSON
2. CDW_SAPP_CREDITCARD.JSON
3. CDW_SAPP_CUSTOMER.JSON

### Req-1.2 Data loading into Database
#### 1.2) Data Loading Into Database
Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL), perform the following:

a) Create a Database in SQL(MariaDB), named “creditcard_capstone.”

b) Create a Python and Pyspark Program to load/write the “Credit Card System Data” into RDBMS(creditcard_capstone)
- Tables should be created by the following names in RDBMS:
	- CDW_SAPP_BRANCH
	- CDW_SAPP_CREDIT_CARD
	- CDW_SAPP_CUSTOMER

## 2. Functional Requirements - Application Front-End
Once data is loaded into the database, we need a front-end (console) to see/display data. For that, create a console-based Python program to satisfy System Requirements 2 (2.1 and 2.2).
## 2.1 Transaction Details Module
### Req-2.1 Transaction Details Module
#### 2.1) Transaction Details Module
1) Display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
2) Display the number and total values of transactions for a given type.
3) Display the number and total values of transactions for branches in a given state.

## 2.2 Customer Details Module
### Req-2.2 Customer Details
#### 2.2) Customer Details Module
1) Check the existing account details of a customer.
2) Modify the existing account details of a customer.
3) Generate a monthly bill for a credit card number for a given month and year.
4) Display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

## 3 - Functional Requirements - Data analysis and Visualization

After data is loaded into the database, users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the data according to the below requirements. Use Python libraries for the below requirements:
### Req - 3 Credit Card Data Analysis and Visualization

3.1) Find and plot which transaction type has a high rate of transactions.

3.2) Find and plot which state has a high number of customers.

3.3) Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.


### Overview of LOAN application Data API
Banks deal in all home loans. They have a presence across all urban, semi-urban, and rural areas. Customers first apply for a home loan; after that, a company will validate the customer's eligibility for a loan.

Banks want to automate the loan eligibility process (in real-time) based on customer details provided while filling out the online application form. These details are Gender, Marital Status, Education, Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of identifying the customer segments to those who are eligible for loan amounts so that they can specifically target these customers. Here they have provided a partial dataset.

#### API Endpoint: [https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json](https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json)

The above URL allows you to access information about loan application information. This dataset has all of the required fields for a loan application. You can access data from a REST API by sending an HTTP request and processing the response.

### 4. Functional Requirements - LOAN Application Dataset
### Req-4 Access to Loan API Endpoint
4.1) Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.

4.2) Find the status code of the above API endpoint.

4.3) Once Python reads data from the API, utilize PySpark to load data into RDBMS(SQL). The table name should be "CDW-SAPP_loan_application" in the database. Use the “creditcard_capstone” database.


## 5 - Functional Requirements - Data Analysis and Visualization for Loan Application
After the data is loaded into the database, the business analyst team wants to analyze and
visualize the data according to the below requirements.
Use Python libraries for the below requirements:
### Req-5 Data Analysis and Visualization
5.1) Find and plot the percentage of applications approved for self-employed applicants.

5.2) Find the percentage of rejection for married male applicants.

5.3) Find and plot the top three months with the largest transaction data.

5.4) Find and plot which branch processed the highest total dollar value of healthcare transactions.
