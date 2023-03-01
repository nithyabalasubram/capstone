# Importing the required libraries.

import findspark
findspark.init()
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import*
from pyspark.sql.types import StringType, IntegerType, BooleanType, DoubleType
import pandas as pd
import mysql.connector as mariadb
from mysql.connector import Error
import os
import re
import matplotlib.pyplot as plt
import random
import requests
from decouple import config


# Creating Spark Session.
spark = SparkSession.builder.appName('Capstone').getOrCreate()


# Function to get the records from database tables.
def get_records_from_database_table(USER, PWD, tablename):
   
   # Getting data from the table    
    df_table=spark.read.format("jdbc")    \
             .options(driver="com.mysql.cj.jdbc.Driver",\
                      user=USER,\
                      password=PWD,\
                      url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                      dbtable =tablename).load()
    
    
    # Return the database table read above.
    return df_table


# Function to fetch all the creditcard, customer and branch data from the creditcard_capstone database.
def get_creditcard_info(USER, PWD):
      
    # Function to get the records from creditcard database table in the 
    # creditcard_capstone database.
    df_transactions= get_records_from_database_table(USER, PWD, "creditcard_capstone.cdw_sapp_credit_card")


    # Function to get the records from customer database table in the 
    # creditcard_capstone database.
    df_customers= get_records_from_database_table(USER, PWD, "creditcard_capstone.cdw_sapp_customer")

    
    # Function to get the records from branch database table in the 
    # creditcard_capstone database.
    df_branches= get_records_from_database_table(USER, PWD, "creditcard_capstone.cdw_sapp_branch")

    
    # Returning the creditcard transaction, customer and branch data fetched from the creditcard_capstone database.
    return df_transactions, df_customers, df_branches



# Function to extract the customer, branch and the creditcard data from the files.
def extract():
    # Reading the customer json file into the spark dataframe df_customer.
    df_customer = spark.read.json("json_files/cdw_sapp_custmer.json")

    # Reading the branch json file into the spark dataframe df_branch.
    df_branch = spark.read.json('json_files/cdw_sapp_branch.json')

    # Reading the credit card json file into the pandas dataframe df_creditcard.
    df_creditcard = pd.read_json("json_files/cdw_sapp_credit.json", lines=True)

    # Returning the customer, branch and the creditcard data after reading from the input file.
    return df_customer, df_branch, df_creditcard




# Function to transform the customer, branch and the creditcard data as per the mapping logic.
def transform(df_customer, df_branch, df_creditcard):
    
    # Customer data.

    df_customer = df_customer.select(col("SSN").cast("int"), initcap(col("FIRST_NAME")).alias("FIRST_NAME"), lower(col("MIDDLE_NAME")).alias("MIDDLE_NAME"), \
                            initcap(col("LAST_NAME")).alias("LAST_NAME"), col("CREDIT_CARD_NO"), \
                            concat_ws(',', col("APT_NO"), col("STREET_NAME")).alias("FULL_STREET_ADDRESS"), \
                            col("CUST_CITY"), col("CUST_STATE"), col("CUST_COUNTRY"), col("CUST_ZIP").cast("int"), \
                            regexp_replace(col("CUST_PHONE"), r'^(\d{3})(\d{4})$', '(214)$1-$2').alias('CUST_PHONE'), \
                            col("CUST_EMAIL"), col("LAST_UPDATED").cast("timestamp"))

    # Branch data.

    # Creating the temporary view for the branch data.
    df_branch.createOrReplaceTempView("branchtable")

    # Getting all the records from the branchtable created above and applying the mapping logic.
    df_branch = spark.sql("SELECT CAST(BRANCH_CODE AS INT), BRANCH_NAME, BRANCH_STREET, BRANCH_CITY, \
                          BRANCH_STATE, CAST(IF(BRANCH_ZIP IS NULL, '99999', BRANCH_ZIP) AS INT) AS BRANCH_ZIP, \
                          CONCAT('(', SUBSTR(BRANCH_PHONE, 1, 3), ')', SUBSTR(BRANCH_PHONE, 4,3), '-', SUBSTR(BRANCH_PHONE, 7, 4)) AS BRANCH_PHONE, \
                          CAST(LAST_UPDATED AS TIMESTAMP) FROM BRANCHTABLE")


    # Creditcard data.

    # Converting the data types and renaming the columns as per the mapping logic.
    df_creditcard = df_creditcard.astype({"DAY":'str',"MONTH":'str',"YEAR":'str', "CREDIT_CARD_NO":'str'})
    df_creditcard.rename(columns={"CREDIT_CARD_NO" : "CUST_CC_NO"}, inplace=True)
    df_creditcard['DAY'] = df_creditcard['DAY'].str.zfill(2)
    df_creditcard['MONTH'] = df_creditcard['MONTH'].str.zfill(2)    

    # Combining the day, month and year to form the TIMEID.
    df_creditcard['TIMEID'] = df_creditcard['YEAR'] + df_creditcard['MONTH'] + df_creditcard['DAY']

    # Dropping the Day, Month and Year columns from the creditcard data.
    df_creditcard.drop(columns=['DAY', 'MONTH', 'YEAR'], axis = 1, inplace = True)

    # Converting the credit card pandas dataframe into spark dataframe.
    df_creditcard = spark.createDataFrame(df_creditcard)

    # Converting the columns Branch Code, Cust SSN, Transaction ID to the integer data type.
    df_creditcard= df_creditcard.withColumn("BRANCH_CODE", df_creditcard["BRANCH_CODE"].cast("int"))
    df_creditcard = df_creditcard.withColumn("CUST_SSN", df_creditcard["CUST_SSN"].cast("int"))
    df_creditcard = df_creditcard.withColumn("TRANSACTION_ID", df_creditcard["TRANSACTION_ID"].cast("int"))

    
    # Return the transformed customer, branch and creditcard data.
    return df_customer, df_branch, df_creditcard




# Function to load the customer, branch and the creditcard data to the database creditcard_capstone.
def load(df_customer_data, df_branch_data, df_creditcard_data):

    # Writing customer data to the customer database table.
    df_customer_data.write.format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate", "true") \
                    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                    .option("createTableColumnTypes", "FIRST_NAME VARCHAR(30), MIDDLE_NAME VARCHAR(30), \
                    LAST_NAME VARCHAR(30), CREDIT_CARD_NO VARCHAR(20), FULL_STREET_ADDRESS VARCHAR(50), \
                    CUST_CITY VARCHAR(30), CUST_STATE VARCHAR(5), CUST_COUNTRY VARCHAR(30), \
                    CUST_PHONE VARCHAR(20), CUST_EMAIL VARCHAR(30)") \
                    .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
                    .option("user", USER) \
                    .option("password", PWD) \
                    .save()

# Writing branch data to the branch database table.
    df_branch_data.write.format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate", "true") \
                    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                    .option("createTableColumnTypes", "BRANCH_NAME VARCHAR(30), BRANCH_STREET VARCHAR(50), \
                     BRANCH_CITY VARCHAR(30), BRANCH_STATE VARCHAR(5), BRANCH_PHONE VARCHAR(20)") \
                    .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
                    .option("user", USER) \
                    .option("password", PWD) \
                    .save()


# Writing creditcard data to the creditcard database table.
    df_creditcard_data.write.format("jdbc") \
                    .mode("overwrite") \
                    .option("truncate", "true") \
                    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                    .option("createTableColumnTypes", "CUST_CC_NO VARCHAR(20), TIMEID VARCHAR(10), \
                    TRANSACTION_TYPE VARCHAR(30)") \
                    .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
                    .option("user", USER) \
                    .option("password", PWD) \
                    .save()




# Function to print the schema in a tree structure for the dataframe data provided.
def print_schema(df):
    df.printSchema()
    return ""



# Function to print the data types of the columns in the dataframe for the given data.
def print_data_types(df):
    print(df.dtypes)
    return ""



# Function to display the records with all the information.
def display_data(df_type):
    df_type.show(5)
    return ""



# Function to extract, transform and load the data read from the files to the database.
def extract_transform_load():    

    # Extract, transform and load data to the creditcard_capstone database.

    # Extract.
    print("Extraction started")

    # Extract the customer, branch and creditcard transaction data.
    df_customer, df_branch, df_creditcard = extract()

    print("\nExtraction completed")

    # Transform.
    print("\nTransform started")

    # Transform the customer, branch and creditcard transaction data.
    df_customer_data, df_branch_data, df_creditcard_data = transform(df_customer, df_branch, df_creditcard)

    print("\nTransform completed")

    # Displaying the schemas after transformation.
    print("\nDisplaying the schema after transformation")
    print("\nCustomer schema after transformation:")
    print_schema(df_customer_data)
    print("\nBranch schema after transformation:")
    print(print_schema(df_branch_data))
    print("\nCreditcard schema after transformation:")
    print(print_data_types(df_creditcard_data))

    # Displaying the customer, branch and creditcard transaction data after transformation
    # and before loading it to the creditcard_capstone database.
    print("\nDisplaying the data before loading to the database")
    
    # Displaying customer data.
    print("\nCustomer data:")
    display_data(df_customer_data)

    # Displaying the branch data.
    print("\nBranch data:")
    display_data(df_branch_data)

    # Displaying the creditcard data.
    print("\nCreditcard data:")
    display_data(df_creditcard_data)

    # Loading.
    print("\nLoading started")

    # Load the customer, branch and creditcard transaction data.
    load(df_customer_data, df_branch_data, df_creditcard_data)
    
    print("\nLoading completed")   



# Function to populate the customer, branch and creditcard tables only if it does not exist
# in the creditcard_capstone database.

def populate_tables_only_if_not_exist(USER, PWD):
    
    # Checking if the customer, branch and creditcard table exists in the creditcard_capstone database.
    # Extract, transform and load the customer, branch and creditcard data only if table does not exists in the database.
    df_table= get_records_from_database_table(USER, PWD, "information_schema.tables")
    df_table = df_table.filter("table_schema = 'creditcard_capstone'")
   
  
    # Check if the tables are created in the database.
    if df_table.isEmpty():
        try:
            extract_transform_load()
        except Error as e:
            print("Error Message:", e)



# Function to get the transaction data into pandas dataframe.
# def get_data_for_transactions(df_transactions, df_customers, df_branches):

def get_data_for_transactions(USER, PWD):

    # Fetching all the creditcard transaction, customer and branch data from the creditcard_capstone database.
    df_transactions, df_customers, df_branches = get_creditcard_info(USER, PWD)

    # Creating a temporary view for the creditcard transaction data.
    df_transactions.createOrReplaceTempView("creditview")

    # Creating a temporary view for the customer data.
    df_customers.createOrReplaceTempView("customerview")

    # Creating a temporary view for the branch data.
    df_branches.createOrReplaceTempView("branchview")

 
    # Transaction data.
    df_trans = df_transactions.toPandas()
    
    # Customer data.
    df_cust = df_customers.toPandas()
    
    # Branch data.
    df_brch = df_branches.toPandas()


    # Getting the credit card transactions for customers.
    df_customer_cc_info = pd.merge(df_cust, df_trans, how='inner', left_on='CREDIT_CARD_NO', right_on='CUST_CC_NO')

    # Getting the transactions for branches.
    df_branch_cc_info = pd.merge(df_brch, df_trans,  how='inner', on='BRANCH_CODE')

    # Getting the list of columns to be displayed for modification.
    customer_columns_info = df_cust.columns.tolist()
    

    # Removing the unwanted columns that should not be given as option to be modified.
    customer_columns_info.remove('LAST_UPDATED')
    customer_columns_info.remove('SSN')
    customer_columns_info.remove('CREDIT_CARD_NO')
    customer_columns_info.remove('CUST_COUNTRY')
    customer_columns_info.remove('FIRST_NAME')
    customer_columns_info.remove('LAST_NAME')
    customer_columns_info.remove('MIDDLE_NAME')


    # Getting the unique values of the transaction type as a list.
    tran_types = pd.unique(df_trans['TRANSACTION_TYPE'])


    # Getting the unique values of the state as a list.
    branch_state = pd.unique(df_brch['BRANCH_STATE'])


    # Getting the unique values of the SSN as a list.
    ssn_number = list(df_cust['SSN'].unique())


    # Getting the unique values of the SSN as a list.
    cc_number = pd.unique(df_cust['CREDIT_CARD_NO'])


    # Getting the unique values of the SSN as a list.
    first_name = pd.unique(df_cust['FIRST_NAME'])


    # Getting the unique values of the SSN as a list.
    last_name = pd.unique(df_cust['LAST_NAME'])


    # Populate states in a list to validate US State in Customer modify.
    usstates = [ 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
           'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
           'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
           'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
           'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']



    # Returning the creditcard transaction, customer, branch data as pandas dataframe.
    return df_trans, df_cust, df_brch, df_customer_cc_info, df_branch_cc_info, customer_columns_info, tran_types, branch_state, usstates,  ssn_number, cc_number, first_name, last_name


# Function to validate month
def validate_month(text):

    # Validating the month.
    while True:

        # Getting the month as input.
        month = input("\nPlease enter the {} in 2 digits: ".format(text))
        month = month.strip()

        # Checking whether the month contains only numbers.
        if month.isdigit() and len(month) == 2 and month != '00':

            # Checking whether the month is in the range of 0 to 12.
            if int(month.lstrip('0')) in range(0,13):
                # If the month is only numbers and in the range of 1 and 12 then it is a valid month.
                print('\nMonth {} is valid.'.format(month))
                break
            else:
                # If the month is not in the range of 1 and 12 then it is an invalid month.
                print("\nMonth {} is invalid. Try Again.".format(month)) 
        else:
            # If the month does not have only numbers then it is an invalid month.
            print("\nInvalid entry {}. Try Again.".format(month))    

    return month



# Function to validate day.
def validate_day(text):
  
    # Validating the day.
    while True:

        # Getting the day as input.
        day = input('\nPlease enter {} in 2 digits: '.format(text))
        day = day.strip()

        # If the day has only numbers and is of length 2 and not equal to zero.
        if day.isdigit() and len(day) == 2 and day != '00':
            # If the day is between 1 and 31 then it is valid.
            if int(day.lstrip('0')) in range(0,32):
                print('\nDay {} is valid'.format(day))
                break
            else:
                # If the day is not between 1 and 31 then the day is invalid.
                print('\nDay {} is invalid. Try again.'.format(day))
        else:
            # If the day does not have only numbers and not of length 2 and is equal to zero then the day is invalid.
            print('\nInvalid entry {}. Try again.'.format(day))

    return day



# Function to validate year.
def validate_year(text):


    # Validating the year.
    while True:
         
        # Getting the year as input.
        year = input("\nPlease enter the {} in 4 digits: ".format(text))
        year = year.strip()
       
        # Checking whether the year contains only numbers and is of length 4.
        if year.isdigit() and len(year) == 4:
            # If year is a number and has 4 digits then it is a valid year.
            print('\nYear {} is valid.'.format(year))
            year = int(year)
            break   
        else:
            # If the year does not have only numbers then it is an invalid year.
            print("\nYear {} is invalid. Try Again.".format(year))
            
    return year



# Function to validate zipcode.
def validate_zipcode():   

   # Validating the zipcode.
   while True:

        # Getting the zipcode as input.
        zipcode = input("\nPlease enter the ZIP code in 4 or 5 digits: ")
        zipcode = zipcode.strip()

        # Checking if the zipcode has only numbers and is of length 5.
        if zipcode.isdigit() and (len(zipcode) == 4 or len(zipcode) == 5):
           zipcode = int(zipcode)
           # If the zipcode has only numbers and is of length 5 then it is a valid zipcode.
           print('Zipcode {} is valid.'.format(zipcode))
           break
        else:
           # If the zipcode does not have only numbers then it is a invalid zipcode.
           print('\nZipcode {} is invalid. Try again.'.format(zipcode))

   return zipcode



# Transaction module.

# Function to display the transactions made by customers living in a
# given zip code for a given month and year. Order by day in
# descending order.

def transaction_customer_for_zip_month_year():

    # Validating the zipcode.
    zipcode = validate_zipcode()
    
    # Validating the month.
    month = validate_month('month')
    
    # Validating the year.
    year = validate_year('year')


    # Fetching the records from the creditview and customerview 
    # to display the transactions made by customers living in a
    # given zip code for a given month and year. Order by day in
    # descending order.
    
    df_result = spark.sql("SELECT \
    CU.FIRST_NAME, CU.MIDDLE_NAME, CU.LAST_NAME, \
    REPLACE(CR.CUST_CC_NO, SUBSTR(CR.CUST_CC_NO, 1, 12), '************') AS CREDIT_CARD_NO, \
    CONCAT(SUBSTR(CR.TIMEID, 1, 4), '-', SUBSTR(CR.TIMEID, 5, 2), '-', SUBSTR(CR.TIMEID, 7, 2)) AS TRANSACTION_DATE, \
    CR.BRANCH_CODE, CR.TRANSACTION_TYPE, ROUND(CR.TRANSACTION_VALUE, 2) AS TRANSACTION_VALUE, \
    CR.TRANSACTION_ID \
    FROM CREDITVIEW AS CR \
    JOIN CUSTOMERVIEW AS CU \
    ON CR.CUST_CC_NO = CU.CREDIT_CARD_NO \
    WHERE CU.CUST_ZIP = {} \
    AND  MONTH(TO_DATE(CR.TIMEID, 'yyyyMMdd')) = '{}' \
    AND  YEAR(TO_DATE(CR.TIMEID, 'yyyyMMdd')) = '{}' \
    ORDER BY DAY(TO_DATE(CR.TIMEID, 'yyyyMMdd')) DESC".format(zipcode, month, year))


    print("\nThe Transactions made by customers living in the zip code {} for the month {} and year {}:".format(zipcode, month, year)) 
    
    # Displaying the customer transaction details.
    if df_result.isEmpty():
        print('\nNo records matching the zipcode {}, month {} and year {} found.'.format(zipcode, month, year))

    df_result.show()



# Function to display the number and total values of transactions for a given transaction type.
def number_and_total_values_of_transactions(tran_type):

   
    # Validating the Transaction type.
    while True:

        # Getting the transaction type as input.
        transaction_type = input("\nPlease enter a Transaction Type: ")
        transaction_type = transaction_type.strip().title()

        # Validating whether the entered transaction type exists in the database.
        if transaction_type in tran_type:
        # If exists then it is a valid transaction type.
            print("\nTransaction type {} is valid.".format(transaction_type))
            break
        else:
        # If the transaction type does not exists in the database then it is not a valid one.
            print("\nTransaction type {} not found. Try Again".format(transaction_type))

    # Displaying the number and total values of transactions for a given transaction type.
    df_result = spark.sql("SELECT TRANSACTION_TYPE, COUNT(TRANSACTION_ID) AS NUMBER_OF_TRANSACTIONS, \
    ROUND(SUM(TRANSACTION_VALUE), 2) AS TRANSACTION_VALUE FROM CREDITVIEW \
    WHERE TRANSACTION_TYPE = '{}' \
    GROUP BY TRANSACTION_TYPE".format(transaction_type))


    print("\nThe Transaction details for the Transaction Type {}:". format(transaction_type))


    # Displaying the transaction details for a given transaction type.
    if df_result.isEmpty():
        print('\nNo records matching the transaction type {} found.'.format(transaction_type))

    df_result.show()



# Function to display the number and total values of transactions for branches in a given state.
def number_and_total_transaction_values_for_branches(branch_states):


    # Validating the State
    while True:
        # Getting the state as input.
        state = input('\nPlease enter state in 2 letters: ')
        # Removing the leading and trailing spaces.
        state = state.strip().upper()

        # Checking whether the state has only letters and the length is 2.
        if state.isalpha() and len(state) == 2:
            # Converting the state to upper case.
            if state in branch_states:
            # If the State exists in the database then it is a valid state.
                print('\nBranch(es) found for the state {}.'.format(state))
                break
            else:
                # If State does not exist in the table then no branch found.
                print('\nBranches for the state {} not found. Try again.'.format(state))
        else:
            # If the State entered does not have only letters or not of length 2 then it
            # is not a valid State.
            print('\nState {} is invalid. Try again.'.format(state))


    # Display the number and total values of transactions for
    # branches in a given state.
    df_result = spark.sql("SELECT COUNT(C.TRANSACTION_ID) AS NUMBER_OF_TRANSACTIONS, \
    ROUND(SUM(C.TRANSACTION_VALUE), 2) AS TRANSACTION_VALUE \
    FROM BRANCHVIEW AS B \
    JOIN CREDITVIEW AS C ON B.BRANCH_CODE = C.BRANCH_CODE \
    WHERE B.BRANCH_STATE ='{}' \
    GROUP BY B.BRANCH_NAME".format(state))

    print("\nThe number and total values of transactions for branches in the State {}:".format(state))

    # Displaying the transactions for the branches.
    if df_result.isEmpty():
        print('\nNo data matching criteria.')
        
    df_result.show()



# Function to validate the credit card number.

def validate_credit_card_no(cardnumbers):

    # Validating the credit card number.
    while True:
        cardno = input("\nPlease enter the 16 digit Credit Card Number: ")
        cardno = cardno.strip()

        # Checking the creditcard number has only numbers in it and is of length 16.
        if cardno.isdigit() and len(cardno) == 16:
            if cardno in cardnumbers:
                # If the creditcard number has only numbers and is of length 16 then it is a valid number.
                print("\nCredit Card Number ending with the last 4 digits {} is valid".format(cardno[12:]))
                break
            else:
                # If the credit card does not have only the numbers or not of length 16 then invalid.
                print('\nCredit Card Number ending with the last 4 digits {} not found. Try again.'.format(cardno[12:]))

        else:
            # If the credit card does not have only the numbers or not of length 16 then invalid.
            print('\nCredit Card Number {} is invalid. Try again.'.format(cardno))

    return cardno



# Function to validate phone number.

def validate_phone():

    while True: 
        new_entry = input('\nPlease enter phone number in 10 digits: ')
        new_entry = new_entry.strip()

        # Checking if the phone number has only numbers and is of length 10.
        # If it satisfies the above condition then it is a valid phone number.
        
        if new_entry.isdigit() and len(new_entry) == 10:
            print("\nPhone number {} is valid".format(new_entry))
            new_entry = '(' + new_entry[:3] + ')' + new_entry[3:6] + '-' + new_entry[6:]
            break
                         
        else:
            print('\nPhone number {} is invalid. Try again.'.format(new_entry))

    return new_entry        




# Function to validate SSN.

def validate_ssn(ssnno):

    while True:
        # Get the input for SSN.
        ssn = input('\nPlease enter the customer SSN in 9 digits: ')
        ssn = ssn.replace('-','').replace(' ','')

        # Checking if the SSN has only numbers and is of length 9.
        if ssn.isdigit() and len(ssn) == 9:
            ssn_no = int(ssn)
            # Checking if SSN is valid by checking the valid SSN in the database.
            if ssn_no in ssnno:
                print('\nSSN ending with the last 4 digits {} is valid.'.format(ssn[5:]))
                break
            else:
                print("\nSSN ending with the last 4 digits {} not found. Try Again.".format(ssn[5:]))
        else:
            print('\nSSN {} is invalid. Try again.'.format(ssn))

    return ssn



# Function to validate the firstname.
def validate_firstname(fstname): 
    
# Validating the firstname.
    
    while True:
        firstname = input('\nPlease enter the first name: ')
        firstname = firstname.strip().title()
        if firstname.isalpha():
            if firstname in fstname:
                print('\nFirstname {} is valid.'.format(firstname))
                break
            else:
                print("\nFirstname {} not found. Try Again.".format(firstname))
        else: 
            print('\nFirstname {} is invalid. Try again.'.format(firstname))

    return firstname



# Function to validate the lastname.
def validate_lastname(lstname):

    # Validating the lastname.
    while True:
        lastname = input('\nPlease enter the last name: ')
        lastname = lastname.strip().title()

        # Checking if the firstname has only alphabets.
        if lastname.isalpha():
            # Checking if the firstname is found in the database.
            if lastname in lstname:
                print('\nLastname {} is valid.'.format(lastname))
                break
            else:
                print("\nLastname {} not found. Try Again.".format(lastname))
        else: 
            print('\nLastname {} is invalid. Try Again.'.format(lastname))
            
    return lastname




# Function to check the existing account details of a customer.
def check_customer_account_details(cardnumbers, df_cus, fstname, lstname):

    menu = ''
    opt = ''    
    menu = ('\n'
            '1) Use Credit Card Number to get the account details.\n'
            '2) Use Phone Number to get the account details.\n'
            '3) Use SSN to get the account details.\n')
   
    print(menu)
    opt= input('Please choose option 1 for Credit Card or 2 for Phone Number or 3 for SSNr: ')
    opt = opt.strip()

    # Creditcard number.
    if opt == '1':

        # Validating the credit card number.
        cardno = validate_credit_card_no(cardnumbers)

        # Validate the first name.
        fname = validate_firstname(fstname)
     
        # Validate the last name.
        lname = validate_lastname(lstname) 
    
    # Phone number.    
    elif opt == '2':

        # Validate the phone number.
        phone = validate_phone()

        # Validate the first name.
        fname = validate_firstname(fstname)
     
        # Validate the last name.
        lname = validate_lastname(lstname) 

    # SSN.   
    elif opt == '3':


        # Validate the credit card number.
        ssn = validate_ssn(ssnno)
        ssn_num = int(ssn)

        # Validate the first name.
        fname = validate_firstname(fstname)
     
        # Validate the last name.
        lname = validate_lastname(lstname) 

                        
    # Creditcard number.
    if opt == '1':

        # Fetching the existing account details of a customer from the customer table for the given creditcard number.

        df_result = spark.sql(f"SELECT FIRST_NAME, MIDDLE_NAME, LAST_NAME, \
        REPLACE(CREDIT_CARD_NO, SUBSTR(CREDIT_CARD_NO, 1, 12), '************') AS CREDIT_CARD_NO, \
        FULL_STREET_ADDRESS, CUST_CITY AS CITY, CUST_STATE AS STATE, CUST_COUNTRY AS COUNTRY, CUST_ZIP AS ZIP, \
        CUST_PHONE AS PHONE_NUMBER, CUST_EMAIL AS EMAIL, LAST_UPDATED \
        FROM CUSTOMERVIEW WHERE CREDIT_CARD_NO = '{cardno}' AND FIRST_NAME = '{fname}' AND LAST_NAME = '{lname}'")

    # Phone Number.
    elif opt == '2':

        # Fetching the existing account details of a customer from the customer table for the given phone number.

        df_result = spark.sql(f"SELECT FIRST_NAME, MIDDLE_NAME, LAST_NAME, \
        REPLACE(CREDIT_CARD_NO, SUBSTR(CREDIT_CARD_NO, 1, 12), '************') AS CREDIT_CARD_NO, \
        FULL_STREET_ADDRESS, CUST_CITY AS CITY, CUST_STATE AS STATE, CUST_COUNTRY AS COUNTRY, CUST_ZIP AS ZIP, \
        CUST_PHONE AS PHONE_NUMBER, CUST_EMAIL AS EMAIL, LAST_UPDATED \
        FROM CUSTOMERVIEW WHERE CUST_PHONE = '{phone}' AND FIRST_NAME = '{fname}' AND LAST_NAME = '{lname}'")


    # SSN.
    elif opt == '3':

        # Fetching the existing account details of a customer from the customer table for the given SSN, firstname and lastname.

        df_result = spark.sql(f"SELECT FIRST_NAME, MIDDLE_NAME, LAST_NAME, \
        REPLACE(CREDIT_CARD_NO, SUBSTR(CREDIT_CARD_NO, 1, 12), '************') AS CREDIT_CARD_NO, \
        FULL_STREET_ADDRESS, CUST_CITY AS CITY, CUST_STATE AS STATE, CUST_COUNTRY AS COUNTRY, CUST_ZIP AS ZIP, \
        CUST_PHONE AS PHONE_NUMBER, CUST_EMAIL AS EMAIL, LAST_UPDATED \
        FROM CUSTOMERVIEW WHERE SSN = {ssn_num} AND FIRST_NAME = '{fname}' AND LAST_NAME = '{lname}'")

    
                          
    # Checking if record exists in the database table.
    if df_result.isEmpty():

        # Credit card number
        if opt == '1':
            print("\nNo records for the credit card number ending with the last four digits {} with the first name {} and last name {} found.".format(cardno[12:], fname, lname))

        
        # Phone number
        elif opt == '2':
            print("\nNo records for the phone number {} with the first name {} and last name {} found.".format(phone, fname, lname))

        # SSN number    
        elif opt == '3':
            print("\nNo records for the social security number ending with the last four digits {} with the first name {} and last name {} found.".format(ssn[5:], fname, lname))



    # Displaying the existing account details of a customer from the customer table.
    df_result.show()



# Function to validate the email.
def valid_email(email):
    pattern = '^[a-zA-Z0-9-_]+@[a-zA-Z0-9]+\.[a-z]{1,3}$'
    if re.match(pattern, email):
        return True
    else:
        return False



# Function to modify the existing account details of a customer.
def modify_existing_account_details_of_a_customer(customer_columns, ssnno, states, USER, PWD):
  
    # Validating the credit card number.
    ssn = validate_ssn(ssnno)


    # Connecting to the mysql database.
    try:
        mydb = mariadb.connect(host='localhost',
                                    user=USER,
                                    password=PWD,
                                    database='creditcard_capstone')
        
        # checking whether the database is connected.
        if mydb.is_connected():
            db_Info = mydb.get_server_info()
            mycursor = mydb.cursor()

            # Checking whether the customer record dxists in the customer table for the given credit card number.
            ssn_number = int(ssn)
            st ="SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = '{}'"
            mycursor.execute(st.format(ssn_number))
            result= mycursor.fetchall()
           
            # If a customer record exists for the given credit card number then allow to modify.
            if len(result) == 0:
               print('No customer record for the given Social Security number {} found.'.format(ssn[5:]))
            
            else:
                # Populating the dictionary with the selected record details from the database. 
                cus_data = {}
                apt = ''
                aptno = ''

                # Populating the dictionary with the selected values from the database for the entered SSN.
                cus_data['SSN'] = result[0][0]
                cus_data['CREDIT_CARD_NO'] = result[0][4]
                cus_data['FULL_STREET_ADDRESS'] = result[0][5]
                cus_data['CUST_CITY'] = result[0][6]
                cus_data['CUST_STATE'] = result[0][7]
                cus_data['CUST_COUNTRY'] = result[0][8]
                cus_data['CUST_ZIP'] = result[0][9]
                cus_data['CUST_PHONE'] = result[0][10]
                cus_data['CUST_EMAIL'] = result[0][11]
                cus_data['COUNT'] = 0
                count = 1
                
                # Display the menu of customer fields to be modified. 
                for c in customer_columns:
                    print(' ' + str(count) + ') ' + c)
                    count += 1
                print(' ' + str(count) + ') Exit')
                
                while True:
                    print('\nPlease choose the option you would like to modify:')
                    option = input('Please enter option: ')
                    option = option.strip()
                    if option.isdigit():
                        if int(option) <= len(customer_columns):
                            column_name = customer_columns[int(option)-1]
                            if option == '1': # FULL_STREET_ADDRESS
                                 while True: 
                                    new_entry = input('\nPlease enter new ' + column_name + ': ')
                                    new_entry = new_entry.strip()
                                    aptno = new_entry.split()[0]
                                    apt = new_entry.split(',')[0]
                                    
                                    # Checking if it is has only digits and is of address structure.
                                    if apt.isdigit() or aptno.isdigit():
                                        if re.match(r"\d+,?(?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?", new_entry):
                                            cus_data['FULL_STREET_ADDRESS'] = new_entry 
                                            cus_data['COUNT'] += 1  
                                            break
                                        else:
                                            print('\nFull street address {} is invalid. Try again.'.format(new_entry))
                                    else:
                                        print('\nFull street address {} is invalid. Please enter Apartment No. or House Number in the beginning. Try again.'.format(new_entry))
                                    
                            elif option == '2': # CUST_CITY
                                while True: 
                                    new_entry = input('\nPlease enter new ' + column_name + ': ')
                                    new_entry = new_entry.strip().title()
                                    if new_entry.isalpha():
                                       cus_data['CUST_CITY'] = new_entry
                                       cus_data['COUNT'] += 1  
                                       break
                                    else:
                                        print('\nCity {} is invalid. Please enter only letters. Try again.'.format(new_entry))

                            elif option == '3': # CUST_STATE
                                while True:
                                    new_entry = input('\nPlease enter new ' + column_name + ' in 2 letters: ')
                                    new_entry = new_entry.strip().upper()
                                    if new_entry.isalpha() and len(new_entry) == 2 and new_entry in states:
                                       cus_data['CUST_STATE'] = new_entry
                                       cus_data['COUNT'] += 1  
                                       break
                                    else:
                                        print('\nState {} is invalid. Try again.'.format(new_entry))

                            elif option == '4': # CUST_ZIP
                                  
                                  new_entry = validate_zipcode()
                                  cus_data['CUST_ZIP'] = int(new_entry)
                                  cus_data['COUNT'] += 1  
                      
                            elif option == '5': # CUST_PHONE
                                
                                new_entry = validate_phone()
                                cus_data['CUST_PHONE'] = new_entry
                                cus_data['COUNT'] += 1  
                         
                            elif option == '6': # CUST_EMAIL
                                while True: 
                                    new_entry = input('\nPlease enter new ' + column_name + '. Please use the format abc@test.com: ')
                                    new_entry = new_entry.strip()
                                    if valid_email(new_entry):
                                       cus_data['CUST_EMAIL'] = new_entry
                                       cus_data['COUNT'] += 1  
                                       break
                                    else:
                                        print('\nE-mail {} is invalid. Try again.'.format(new_entry))
                        # Exit
                        elif int(option) == count:
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))
                    else:
                        print('\nOption {} is invalid. Try again.'.format(option))

                # Checking the count in the dictionary to see if any field has been modified.
                if cus_data['COUNT'] > 0:
                               
                    # Update the database with the fields modified.
                    updatest= f"UPDATE CDW_SAPP_CUSTOMER \
                    SET FULL_STREET_ADDRESS= '{cus_data['FULL_STREET_ADDRESS']}', \
                    CUST_CITY= '{cus_data['CUST_CITY']}',CUST_STATE = '{cus_data['CUST_STATE']}', \
                    CUST_ZIP= {cus_data['CUST_ZIP']}, CUST_PHONE= '{cus_data['CUST_PHONE']}', \
                    CUST_EMAIL= '{cus_data['CUST_EMAIL']}', LAST_UPDATED= NOW() \
                    WHERE SSN = {cus_data['SSN']}"

                    # Modifying the database.
                    mycursor.execute(updatest)
                    # Save it to the database.
                    mydb.commit()
                                
                    # Checking if there are any records that got updated and displaying the modified record.
                    if mycursor.rowcount > 0:
                        ssn_number = int(ssn)
                        print('\n{} Customer record modified for the Social Security Number ending with the last four digits {}.'.format(mycursor.rowcount, ssn[5:]))

                        
                        st1 ="SELECT FIRST_NAME, MIDDLE_NAME, LAST_NAME, REPLACE(CREDIT_CARD_NO, SUBSTR(CREDIT_CARD_NO, 1, 12), '************') AS CREDIT_CARD_NO, \
                        FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, \
                        CUST_EMAIL, LAST_UPDATED \
                        FROM CDW_SAPP_CUSTOMER WHERE SSN = '{}'"


                        mycursor.execute(st1.format(ssn_number))
                        output= mycursor.fetchall()
                        print(output[0])

                    else:
                        print('\nNo Customer record modified for the Social Security Number ending with the last four digits {}.'.format(ssn[5:])) 

    
    # Getting the error.           
    except Error as e:
        print("Error while connecting to Database", e)
    
    finally:

        # Closing the cursor and database connection after updating the customer data.
        if mydb.is_connected():
            mycursor.close()
            mydb.close()

            
   
# Function to generate a monthly bill for a credit card number for a given month and year.
def credit_card_monthly_bill_for_a_month_and_year(cardnumbers):

    # Validating the credit card number.
    cardno = validate_credit_card_no(cardnumbers)
        
    # Validating the month.
    month = validate_month('month')
    
    # Validating the year.   
    year = validate_year('year')
   
    #  Fetching the customer transaction data for a credit card number for a
    # given month and year.
    df_result = spark.sql(f"SELECT REPLACE(CUST_CC_NO, SUBSTR(CUST_CC_NO, 1, 12), ('************')) AS CREDIT_CARD_NO, \
    ROUND(SUM(TRANSACTION_VALUE), 2) AS TOTAL_TRANSACTION_VALUE \
    FROM CREDITVIEW WHERE CUST_CC_NO = '{cardno}' AND MONTH(TO_DATE(TIMEID, 'yyyyMMdd')) = '{month}' \
    AND YEAR(TO_DATE(TIMEID, 'yyyyMMdd')) = '{year}' \
    GROUP BY CUST_CC_NO")


    # Checing whether an entry exists in the database for the given input.
    if df_result.isEmpty():
        print('\nNo record matching the credit card number ending with the last four digits {} for \
            the month {} and year{} found.'.format(cardno[12:], month, year))
        

    # Displaying the credit card transaction details for a
    # given month and year.

    df_result.show()



# Function to display the transactions made by a customer between two dates.
# Order by year, month and day in descending order.

def transactions_by_a_customer_between_dates(ssnno):

   
    # Validating the credit card number.
    cardno = validate_credit_card_no(cardnumbers)


    # # Validating the credit card number.
    # cardno = validate_credit_card_no()

    # Validating the startday.
    startday = validate_day('start day')

    # Validating the startmonth.
    startmonth = validate_month('start month')
        
    # Validating the startyear.
    startyear = validate_year('start year')

    # Validating the endday.
    endday = validate_day('end day')
  
    # Validating the endmonth.
    endmonth = validate_month('end month')
        
    # Validating the endyear.
    endyear = validate_year('end year')
  
    # Concatenating the year, month and day to get the timeid.
    startyear = str(startyear)
    endyear = str(endyear)
    starttimeid = startyear + startmonth + startday
    endtimeid = endyear + endmonth + endday

    startdate = startyear + '-' + startmonth + '-' + startday
    enddate = endyear + '-' + endmonth + '-' + endday

 
   
    # Fetching the records to display the transactions made by a customer between
    # two dates. Order by year, month, and day in descending order.

    df_result = spark.sql(f"SELECT REPLACE(CUST_CC_NO, SUBSTR(CUST_CC_NO, 1, 12), '************')  AS CREDIT_CARD_NO, \
    CONCAT(SUBSTR(TIMEID, 1, 4), '-', SUBSTR(TIMEID, 5, 2), '-', SUBSTR(TIMEID, 7, 2)) AS TRANSACTION_DATE, \
    BRANCH_CODE, TRANSACTION_TYPE, ROUND(TRANSACTION_VALUE, 2) AS TRANSACTION_VALUE, \
    TRANSACTION_ID FROM CREDITVIEW \
    WHERE CUST_CC_NO = '{cardno}' AND (TIMEID >= '{starttimeid}' \
    AND TIMEID <= '{endtimeid}') \
    ORDER BY YEAR(TO_DATE(TIMEID, 'yyyyMMdd')) DESC, MONTH(TO_DATE(TIMEID, 'yyyyMMdd')) DESC, DAY(TO_DATE(TIMEID, 'yyyyMMdd')) DESC")

    # Checking whether an entry exists in the database for the given input.
    if df_result.isEmpty():
        
        print('\nNo records matching the creditcard number ending with the last 4 digits {} with the start date {} and the end date {} found.'.format(cardno[12:], startdate, enddate))
    
    # displaying the transactions made by a customer between
    # two dates. Order by year, month, and day in descending order.

    df_result.show()



# Visualization Module.

# Function to set colors for the plot.
def set_colors_for_plot(length):
    return ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(length)]


# Function to plot which transaction type has a high rate of transactions.
def high_rate_of_tran_by_tran_type(df_tran): 
   
    # Getting the count of the number of trsnsactions per transaction type.
    types = df_tran['TRANSACTION_TYPE'].value_counts()


    # Setting the number of colors to the count of the records taken above.
    no_of_colors = set_colors_for_plot(len(types))
    types.sort_values(ascending=True, inplace = True)


    # Plotting the transaction type that has a high rate of transactions.
    types.plot(kind='barh', figsize=(12, 5),  xlim=(6000,7000), color = no_of_colors)

 
    # Setting the title of the plot.
    plt.title('Total Transactions Per Transaction Type', fontweight = 'bold', fontsize = 18)
    # Setting the x-label.
    plt.xlabel('Rate of Transactions', fontweight = 'bold', fontsize = 14)
    # Setting the y-label.
    plt.ylabel('Transaction Types', fontweight = 'bold', fontsize = 14)
    

    # annotate value labels to each country.
    for index, value in enumerate(types): 
        plt.text(value+10, index-0.1, str(value))
           
    plt.show() 



# Function to find and plot which state has a high number of customers.
def state_with_high_no_of_customers(df_cus):
    
    
    # Getting the count of customers per state.
    states = df_cus['CUST_STATE'].value_counts()


    # Set random colors for each state.
    state_color = set_colors_for_plot(len(states))

    states.sort_values(ascending=True, inplace = True)


    # Plotting the bar graph for the states.
    states.plot(kind='barh', figsize=(10, 5), color = state_color, xlim=(0,100))
 

    # Setting the title.
    plt.title('Total Number of Customers Per State', fontweight = 'bold', fontsize = '18')
    # Setting the x-label.
    plt.xlabel('Total Number of Customers', fontweight = 'bold', fontsize = '14')
    # Setting the y-label.
    plt.ylabel('State', fontweight = 'bold', fontsize = 14)


    # Displaying the number of customers for the state.
    for index, value in enumerate(states): 
      plt.text(value+0.6, index-0.4, str(value))
  
    plt.show() 



# Function to plot the sum of all transactions for the top 10 customers,
# and which customer has the highest transaction amount.
# hint(use CUST_SSN).

def highest_and_sum_of_transactions_for_top10(df_customer_cc):
    

    # Getting the top 10 customers.
    top10 = df_customer_cc.groupby('CUST_SSN')['TRANSACTION_VALUE'].sum().sort_values().tail(10)
  
    # Setting the color for each customer in top 10.
    top10_colors = set_colors_for_plot(len(top10))


    # Plotting the graph for the top 10 customers having the highest total values of transactions.
    top10.plot(kind='barh', figsize=(10, 5), xlim=(4800,5800), color=top10_colors)

    # Placing text i.e. sum of all transactions at the end of bar
    for index, value in enumerate(top10): 
        plt.text(value+10, index-0.25, '$'+str(value))
     

    # Setting the plot.
    plt.title('Top 10 Total Transaction Amounts Per Customer', fontweight = 'bold', fontsize = 18)
    # Setting the x-label.
    plt.xlabel('Total Transaction Amount', fontweight = 'bold', fontsize = 14)
    # Setting the y-label.
    plt.ylabel('Account Number', fontweight = 'bold', fontsize = 14)
    
    plt.show()



# Function to extract the json loan data from the API response.
def api_extract():

    # API url for loan application data.
    loanapi_url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
 
    # Getting the response from the API to get the json file.
    response = requests.get(loanapi_url)

    # Json file is loaded into the loan dataframe.
    loan_data = response.json()

    # Returns the loan data json file and the status code.
    return loan_data, response.status_code 



#Function to print the API status code after reading from the response.
def print_api_status_code(status):
    
    # Printing the status code.
    print('API endpoint status code: ' + str(status))



# Function to  create a spark dataframe and load the loan data to the table CDW_SAPP_loan_application.
def load_loan_data_to_database(loan_info, USER, PWD):
        
        # Creating a Spark DataFrame with the loan application data.
        df_loan_details = spark.createDataFrame(loan_info)

        # Writing the loan application data to the database table CDW_SAPP_loan_application.
        df_loan_details.write.format("jdbc") \
                        .mode("overwrite") \
                        .option("truncate", "true") \
                        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                        .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
                        .option("user", USER) \
                        .option("password", PWD) \
                        .save()



# Function to extract and load loan API.
def loan_data_processing(USER, PWD):

    # Extracting the loan data from the API.
    loan_info, status = api_extract()

    # Displaying the status code to show whether it is successful or not.
    print_api_status_code(status)
    
    # Load the data into the database creditcard_capstone to the table CDW_SAPP_loan_application.
    load_loan_data_to_database(loan_info, USER, PWD)
    


# Function to populate the loan application table only if the table does not exist in the 
# creditcard_capstone database.

def populate_loan_table_only_if_not_exists(USER, PWD):
    
    # Checking if the customer, branch and creditcard table exists in the creditcard_capstone database.
    # Extract and load the loan data from API only if the table CDW_SAPP_loan_application does not exists 
    # in the creditcard_capstone database.
    
    df_tab= get_records_from_database_table(USER, PWD, "information_schema.tables")
    df_tab = df_tab.filter("table_name = 'cdw_sapp_loan_application'")
       
    # Check whether any record is there in the database.
    if df_tab.isEmpty():
        try:
            loan_data_processing(USER, PWD)
        except Error as e:
            print("Error Message:", e)



# Function to fetch all the loan application data from the creditcard_capstone database.
def get_loan_info(USER, PWD):

    # Function to get the records from customer database table in the 
    # creditcard_capstone database.
    df_loans= get_records_from_database_table(USER, PWD, "creditcard_capstone.cdw_sapp_loan_application")
    df_loan = df_loans.toPandas()
    
    
    return df_loan



# Function to plot the percentage of applications approved for self-employed
# applicants.

def approved_applications_for_self_employed(df_loan_data):


     # Getting the loan applicstion data for the application approved for self-employed applicants.,
     df_loan_info = df_loan_data[['Application_Status','Self_Employed']].value_counts()

     colors_list = ['yellowgreen', 'lightcoral', 'lightskyblue', 'gold'] 
     
        
     # Plotting the pie chart for the percentage of applications approved for self-employed applicants.
     df_loan_info.plot(kind='pie',
                             figsize=(12, 6),
                             autopct='%1.2f%%', 
                             startangle=90,
                             labels=None,         # turn off labels on pie chart
                             pctdistance=1.12,    # the ratio between the center of each pie slice and the start of the text generated by autopct 
                             legend = True,
                             explode=(0, 0, 0.1, 0),
                             colors=colors_list
                             )


     # Scale the title up by 12% to match pctdistance
     plt.title('Percentage of Applications Approved for Self-Employed Applicants', y=1.07, fontweight = 'bold', fontsize = 15) 

     plt.axis('equal') 

     # Adding the legend
     legend_labels = ['Approved Not Self-Employed', 'Not Approved Not Self-Employed', 'Approved Self-Employed', 'Not Approved Self-Employed' ]  
     plt.legend(labels=legend_labels) #, loc = 'upper left' ) 

     plt.show()




# Function to find and plot the percentage of rejection for married male applicants.
def percentage_of_rejection_for_married_male(df_loan_data):

    # Getting the loan application data for the percentage of rejections for married male applicants.
    df_loan = df_loan_data[['Application_Status', 'Married', 'Gender']].value_counts()

    colors_list = ['yellowgreen', 'lightcoral', 'lightskyblue', 'lightgreen', 'pink', 'lightblue', 'lightgrey',  'gold']
    
    # Plotting  a pie chart to show the percentage of rejection for married male applicants.
    df_loan.plot(kind='pie',
                            figsize=(14, 6),
                            autopct='%1.2f%%', 
                            startangle=250,
                            labels=None,         # turn off labels on pie chart
                            pctdistance=1.12,    # the ratio between the center of each pie slice and the start of the text generated by autopct
                            legend = True,
                            explode=(0, 0.1, 0, 0, 0, 0, 0, 0),
                            colors = colors_list
                           )

    # Scale the title up by 12% to match pctdistance
    plt.title('Percentage of Rejections for Married Male Applicants', 
               y=1.07, fontweight = 'bold', fontsize = 18) 

    plt.axis('equal') 

    # Adding the legend
    legend_labels = ['(Approved, Married, Male)', '(Not Approved, Married, Male)', '(Approved, Not Married, Male)', 
                     '(Not Approved, Not Married, Male)', '(Approved, Not Married, Female)', 
                     '(Not Approved, Not Mariied, Female)', '(Approved, Married, Female)', 
                     '(Not Approved, Married, Female)']
                 
    plt.legend(labels=legend_labels) 

    plt.show()



# Function to plot the top three months with the largest transaction data.
def top3_months_largest_transaction_data(df_tran):

    # Getting the top three months with the largest transaction data. 
    df_top3 = df_tran.loc[:, ['TIMEID', 'TRANSACTION_ID']]
    df_top3['TIMEID'] = pd.to_datetime(df_top3['TIMEID'], format='%Y%m%d').dt.month

    top_3 = df_top3['TIMEID'].value_counts().sort_values().tail(3)

    # Set the random colors for the three months.
    top3_colors = set_colors_for_plot(len(top_3))

    # Plotting the top 3 months with the largest transaction data.
    top_3.plot(kind='barh', figsize=(10, 5), xlim=(3900, 3970), color=top3_colors)
    
    # Setting the title.
    plt.title('Top 3 Total Number of Transactions Per Month', fontweight = 'bold', fontsize = 18)
    # Setting the x-label
    plt.xlabel('Number of Transactions', fontweight = 'bold', fontsize = 14)
    # Setting the y-label
    plt.ylabel('Month', fontweight = 'bold', fontsize = 14)

    # Getting the transaction value to be displayed near the bar of the graph
    for index, value in enumerate(top_3):
      plt.text(value+1, index-0.04, str(value))

  
    plt.show()



# Function to plot the branch that processed the highest total dollar value of
# healthcare transactions.
def branch_with_highest_total_dollar_value(df_tran):


    # Getting all the branches with the transaction data and sorting to get the highest total dollar value of healthcare transactions.
    health = df_tran[df_tran['TRANSACTION_TYPE']=='Healthcare'].groupby('BRANCH_CODE', as_index=False)['TRANSACTION_VALUE'] \
                                                                  .sum() \
                                                                  .sort_values(by='TRANSACTION_VALUE')
   
    # Set the random colors for the branches.
    branch_colors = set_colors_for_plot(len(health))

    # Plotting the scatter plot for the branches that processed transactions.
    health.plot(kind='scatter', x='BRANCH_CODE', y='TRANSACTION_VALUE',
                   figsize=(30, 10), color=branch_colors,  xlim=(0, 200))
    
    
    # Setting the title.
    plt.title('Total Dollar Value of Healthcare Transactions Per Branch', fontweight = 'bold', fontsize = 25)
    # Setting the x-label.
    plt.xlabel('Branch Number', fontweight = 'bold', fontsize = 20)
    # Setting the y-label.
    plt.ylabel('Total Value of Healthcare Transactions', fontweight = 'bold', fontsize = 20)
    

    # Displaying the branch that has the highest total with the highest total box.
    health_series = health.set_index('BRANCH_CODE').squeeze()
    for index, value in health_series.items():
        if value == health_series.max(axis=0):
            plt.text(index-8, value-120, 'Highest Total value', bbox=dict(facecolor='yellow',alpha=0.5), fontweight = 'bold')
            plt.text(index-1, value-230, '$' + str(value), bbox=dict(facecolor='yellow',alpha=0.5), fontweight = 'bold')
        plt.text(index+1, value-20, '#'+str(index))
     
    
    plt.show()




if __name__ == '__main__':

# Getting the username and password.

    USER= config('user')
    PWD= config('pwd')


# Initializing the variables.

    option = ''
    run_main_menu = True
    run_t_menu = ''
    run_c_menu = ''
    run_v_menu = ''


# Populating the menu.


    main_menu = ('\n'
                 '1) Banking Transactions Menu\n'
                 '2) Customer Transaction Menu\n'
                 '3) Visualizations Menu\n'
                 '4) Quit\n')
    transaction_menu = ('\n'
                      '1) Customer Transactions by Zipcode, Month and Year\n'
                      '2) Transactions by Transaction Type\n'
                      '3) Transactions for Branches in a given State\n'
                      '4) Previous menu\n'
                      '5) Exit\n')
    customer_menu = ('\n'
                    '1) Check Existing Account Details of a Customer\n'
                    '2) Modify Existing Account Details of a Customer\n'
                    '3) Generate Monthly Credit Card Bill\n'
                    '4) List of Transaction for a Date Range\n'
                    '5) Previous menu\n'
                    '6) Exit\n')
    visualization_menu = ('\n'
                     '1) Rate of Transactions by Transaction Type\n'
                     '2) Number of Customers by State\n'
                     '3) Total Transactions for top 10 Customers\n'
                     '4) Percentage of Approved Applications for Self-Employed Applicants\n'
                     '5) Percentage of Rejection for Married Male Applicants\n'
                     '6) Top Three Months of Largest Transactions\n'
                     '7) Branch with Highest Value of Healthcare Transactions.' 
                     '8) Previous menu\n'
                     '9) Exit\n')

    return_quit_t = ('\n'
                   '1) Return to main menu\n'
                   '2) Return to transaction menu\n'
                   '3) Exit\n')
    return_quit_c = ('\n'
                   '1) Return to main menu\n'
                   '2) Return to customer menu\n'
                   '3) Exit\n')
    return_quit_v = ('\n'
                   '1) Return to main menu\n'
                   '2) Return to visualization menu\n'
                   '3) Exit\n')

   
    # Function to populate the customer, branch and creditcard tables only if it does not exist
    # in the creditcard_capstone database.
    
    populate_tables_only_if_not_exist(USER,PWD)


    # Function to populate the loan application table only if the table does not exist in the 
    # creditcard_capstone database.
    populate_loan_table_only_if_not_exists(USER,PWD)


    # Fetching all the creditcard transaction, customer and branch data from the creditcard_capstone database.
    df_transactions, df_customers, df_branches = get_creditcard_info(USER, PWD)


    # Creating a temporary view for the creditcard transaction data.
    df_transactions.createOrReplaceTempView("creditview")

    # Creating a temporary view for the customer data.
    df_customers.createOrReplaceTempView("customerview")

    # Creating a temporary view for the branch data.
    df_branches.createOrReplaceTempView("branchview")



    # Function to get all the transaction data into pandas dataframe for visualization.

    df_tran, df_cus, df_br, df_customer_cc, df_branch_cc, customer_columns, tran_type, branch_states, states, ssnno, cardnumbers, fstname, lstname = \
    get_data_for_transactions(USER, PWD)


    # Function to fetch all the loan application data from the creditcard_capstone database.
    df_loan_data = get_loan_info(USER, PWD)


    run_main_menu = True

    while run_main_menu:
        run_t_menu = True
        run_c_menu = True
        run_v_menu = True
        print(main_menu)
        option = input('Please enter an option: ')
        option = option.strip()

        # Transaction menu
        if option == '1':
            print(transaction_menu)
            while run_t_menu:
                    option = input('Please enter an option: ')
                    option = option.strip()

                    # Transaction details menu 1
                    if option == '1':

                        # Function to display the transactions made by customers living in a
                        # given zip code for a given month and year. Order by day in
                        # descending order.

                        transaction_customer_for_zip_month_year()

                          
                        while True:
                            print(return_quit_t)
                            option = input('Please enetr an option: ')
                            option = option.strip()
                            if option == '1':
                                run_t_menu = False
                                break
                            elif option == '2':
                                print(transaction_menu)
                                break
                            elif option == '3':
                                run_t_menu = False
                                run_main_menu = False
                                break
                            else:
                                print('\nOption {} is invalid. Try again.'.format(option))


                    # Transaction details menu 2
                    elif option == '2':

                    
                        # Function to display the number and total values of transactions for a given transaction type.
                        number_and_total_values_of_transactions(tran_type)

        
                        while True:
                            print(return_quit_t)
                            option = input('Please enter an option: ')
                            option = option.strip()
                            if option == '1':
                                run_t_menu = False
                                break
                            elif option == '2':
                                print(transaction_menu)
                                break
                            elif option == '3':
                                run_t_menu = False
                                run_main_menu = False
                                break
                            else:
                                print('\nOption {} is invalid. Try again.'.format(option))

                 
                    # Transaction details menu 3
                    elif option == '3':

                            # Function to display the number and total values of transactions for branches in a given state.
                            number_and_total_transaction_values_for_branches(branch_states)


                            while True:
                                print(return_quit_t)
                                option = input('Please enter an option: ')
                                option = option.strip()
                                if option == '1':
                                    run_t_menu = False
                                    break
                                elif option == '2':
                                    print(transaction_menu)
                                    break
                                elif option == '3':
                                    run_t_menu = False
                                    run_main_menu = False
                                    break
                                else:
                                    print('\nOption {} is invalid. Try again.'.format(option))


                    # Exit Transaction menu    
                    elif option == '4':
                        break

                    elif option == '5':
                        run_main_menu = False
                        break


                    else:
                        print('\nOption {} is invalid. Try again.'.format(option))
                        print(transaction_menu)



    # Customer menu
        elif option == '2':
            print(customer_menu)
            while run_c_menu:
                option = input('Please enter an option: ')
                option = option.strip()

                # Customer menu 1
                if option == '1':

                    # Function to check the existing account details of a customer.
                    check_customer_account_details(cardnumbers, ssnno, fstname, lstname)

                    while True:
                        print(return_quit_c)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_c_menu = False
                            break
                        elif option == '2':
                            print(customer_menu)
                            break
                        elif option == '3':
                            run_c_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))

            
                # Customer menu 2
                elif option == '2':

                    # Function to modify the existing account details of a customer.
                    modify_existing_account_details_of_a_customer(customer_columns, ssnno, states, USER, PWD) 
                    
                    # Function to get the records from customer database table in the 
                    # creditcard_capstone database.
                    df_customers= get_records_from_database_table(USER, PWD, "creditcard_capstone.cdw_sapp_customer")
                    
                    # Creating a temporary view for the customer data.
                    df_customers.createOrReplaceTempView("customerview")

                    # Customer data.
                    df_cust = df_customers.toPandas()

                    # Getting the credit card transactions for customers.
                    df_customer_cc_info = pd.merge(df_cust, df_tran, how='inner', left_on='CREDIT_CARD_NO', right_on='CUST_CC_NO')

             
                    while True:
                        print(return_quit_c)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_c_menu = False
                            break
                        elif option == '2':
                            print(customer_menu)
                            break
                        elif option == '3':
                            run_c_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))


                # Customer menu 3
                elif option == '3':

                    # Function to generate a monthly bill for a credit card number for a given month and year. 
                    credit_card_monthly_bill_for_a_month_and_year(cardnumbers)

                
                    while True:
                        print(return_quit_c)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_c_menu = False
                            break
                        elif option == '2':
                            print(customer_menu)
                            break
                        elif option == '3':
                            run_c_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.fornat(option))

                # Customer menu 4
                elif option == '4':
                    
                    # Display the transactions made by a customer between
                    # two dates. Order by year, month, and day in descending order.
                    transactions_by_a_customer_between_dates(ssnno)


                    while True:
                        print(return_quit_c)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_c_menu = False
                            break
                        elif option == '2':
                            print(customer_menu)
                            break
                        elif option == '3':
                            run_c_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))
                
                # Previous Menu
                elif option == '5':
                    break
                
                # Main Menu
                elif option == '6':
                    run_main_menu = False
                    break

                else:
                    print('\nOption {} is invalid. Try again.'.format(option))
                    print(customer_menu)


        # Visualization Menu     
        elif option == '3':
            print(visualization_menu)
            while run_v_menu:
                option = input('Please enter an option: ')
                option = option.strip()

                # Visulaization menu 1
                if option == '1':

                    # Function to plot the transaction type that has high rate of transactions.
                    high_rate_of_tran_by_tran_type(df_tran)
                
         
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))


                # Visualization menu 2
                elif option == '2':

                    # Function to find and plot which state has a high number of customers.
                    state_with_high_no_of_customers(df_cus)

   
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))

                
                # Visualization menu 3
                elif option == '3':

                    # Function to plot the sum of all transactions for the top 10 customers,
                    # and which customer has the highest transaction amount.
                    highest_and_sum_of_transactions_for_top10(df_customer_cc)

           
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))


                # Visualization menu 4
                elif option == '4':

                    # Function to plot the percentage of applications approved for self-employed
                    # applicants.
                    approved_applications_for_self_employed(df_loan_data)

    
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))


                # Visualization menu 5
                elif option == '5':

                    # Function to find and plot the percentage of rejection for married male applicants.
                    percentage_of_rejection_for_married_male(df_loan_data)

          
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))


                # Visualization menu 6
                elif option == '6':

                    # Function to plot the top three months with the largest transaction data.
                    top3_months_largest_transaction_data(df_tran)
                
         
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption is invalid. Try again.'.format(option))


                # Visulaization menu 7
                elif option == '7':

                    # Plot the branch that processed the highest total dollar value of
                    # healthcare transactions.
                    branch_with_highest_total_dollar_value(df_tran)

      
                    while True:
                        print(return_quit_v)
                        option = input('Please enter an option: ')
                        option = option.strip()
                        if option == '1':
                            run_v_menu = False
                            break
                        elif option == '2':
                            print(visualization_menu)
                            break
                        elif option == '3':
                            run_v_menu = False
                            run_main_menu = False
                            break
                        else:
                            print('\nOption {} is invalid. Try again.'.format(option))

               
                # Previous menu
                elif option == '8':
                    break
                
                # Main menu
                elif option == '9':

                    run_main_menu = False
                    break

                else:
                    print('\nOption {} is invalid. Try again.'.format(option))
                    print(visualization_menu)

    
        # Exit menu
        elif option == '4':
            break
    
        else:
            print('\nOption {} is invalid. Try again.'.format(option))

    
    # Stop the spark session.
    spark.stop()









