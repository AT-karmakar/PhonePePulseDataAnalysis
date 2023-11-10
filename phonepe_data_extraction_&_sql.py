import git
import pandas as pd
import csv
import json
import os
import psycopg2

# Clone the GitHub repository to the root directory
os.system("git clone https://github.com/PhonePe/pulse.git")

# DATA EXTRACTION FROM THE PULSE FOLDER, INTO PANDAS DATA FRAMES
# for aggregated_transaction
root_dir = (r"C:\Users\Akash\Desktop\ProjectPhonePePulse\pulse\data")
 # Inittialize empty list for holding the files
data_list = []

#Iterate over state folders
state_dir_path = os.path.join(root_dir, "aggregated", "transaction", "country", "india", "state")
for state_dir in os.listdir(state_dir_path):
    state_path = os.path.join(state_dir_path, state_dir)
    if os.path.isdir(state_path):
        #iterate over all year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path,year_dir)
            if os.path.isdir(year_path):
                #iterate over all the json files for each quater respectively
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path,json_file)) as f:
                            data = json.load(f)
                            #Extract our data
                            for transaction_data in data['data']['transactionData']:
                                row_dict ={
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'Transaction_Type': transaction_data['name'],
                                    'Transaction_Count': transaction_data['paymentInstruments'][0]['count'],
                                    'Transaction_Amount': transaction_data['paymentInstruments'][0]['amount']
                                }
                                data_list.append(row_dict)

#Convert list fo dictonaries to dataframe
df1 = pd.DataFrame(data_list)

#<--------------------------$$$------------------------->
# for aggregated_users
root_dir = (r"C:\Users\Akash\Desktop\ProjectPhonePePulse\pulse\data")
 # Inittialize empty list for holding the files
data_list = []

#Iterate over state folders
state_dir_path = os.path.join(root_dir, "aggregated", "user", "country", "india", "state")
for state_dir in os.listdir(state_dir_path):
    state_path = os.path.join(state_dir_path, state_dir)
    if os.path.isdir(state_path):
        #iterate over all year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path,year_dir)
            if os.path.isdir(year_path):
                #iterate over all the json files for each quater respectively
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path,json_file)) as f:
                            data = json.load(f)           
                            # Check if 'usersByDevice' key exists and is not None in the JSON data
                            users_by_device = data['data'].get('usersByDevice')
                            # Check if 'usersByDevice' key exists in the JSON data
                            if users_by_device:
                                # Iterate over user devices and directly add 'Brand' and 'Count' to the dictionary
                                for user_device in users_by_device:
                                    result_dict = {
                                        "States": state_dir,
                                        "Transaction_Year": year_dir,
                                        "Quarter": int(json_file.split('.')[0]),
                                        "RegisteredUsers": data['data']['aggregated']['registeredUsers'],
                                        "Brand": user_device.get('brand'),
                                        "Count": user_device.get('count')
                                    }
                                data_list.append(result_dict)

#Convert list fo dictonaries to dataframe
df2 = pd.DataFrame(data_list)

#<--------------------------$$$------------------------->

#for map=>transaction 
# Inittialize empty list for holding the files
data_list = []
root_dir = (r"C:\Users\Akash\Desktop\ProjectPhonePePulse\pulse\data")
state_dir_path = os.path.join(root_dir, "map", "transaction", "hover", "country", "india", "state")
for state_dir in os.listdir(state_dir_path):
    state_path = os.path.join(state_dir_path, state_dir)
    if os.path.isdir(state_path):
        #iterate over all year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path,year_dir)
            if os.path.isdir(year_path):
                #iterate over all the json files for each quater respectively
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path,json_file)) as f:
                            data = json.load(f)
                            #Extract our data
                            for hoverDataList in data['data']['hoverDataList']:
                                row_dict ={
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'District': hoverDataList['name'],
                                    'Transaction_Type': hoverDataList['metric'][0]['type'],
                                    'Transaction_Count': hoverDataList['metric'][0]['amount'],
                                }
                                data_list.append(row_dict)

#Convert list fo dictonaries to dataframe
df3 = pd.DataFrame(data_list)


#<--------------------------$$$------------------------->

#for map=>user 
# Inittialize empty list for holding the files
data_list = []
root_dir = (r"C:\Users\Akash\Desktop\ProjectPhonePePulse\pulse\data")
state_dir_path = os.path.join(root_dir, "map", "user", "hover", "country", "india", "state")
for state_dir in os.listdir(state_dir_path):
    state_path = os.path.join(state_dir_path, state_dir)
    if os.path.isdir(state_path):
        #iterate over all year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path,year_dir)
            if os.path.isdir(year_path):
                #iterate over all the json files for each quater respectively
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path,json_file)) as f:
                            data = json.load(f)
                            #Extract our data
                            for district,values in data['data']['hoverData'].items():
                                row_dict ={
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'District': district,
                                    'RegisteredUsers': values['registeredUsers'],
                                    }
                                data_list.append(row_dict)

#Convert list fo dictonaries to dataframe
df4 = pd.DataFrame(data_list)


#<--------------------------$$$------------------------->

#for top=>transactions
# Inittialize empty list for holding the files
data_list = []
root_dir = (r"C:\Users\Akash\Desktop\ProjectPhonePePulse\pulse\data")
state_dir_path = os.path.join(root_dir, "top", "transaction", "country", "india", "state")
for state_dir in os.listdir(state_dir_path):
    state_path = os.path.join(state_dir_path, state_dir)
    if os.path.isdir(state_path):
        #iterate over all year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path,year_dir)
            if os.path.isdir(year_path):
                #iterate over all the json files for each quater respectively
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path,json_file)) as f:
                            data = json.load(f)
                            #Extract our data
                            for districts in data['data']['districts']:
                                row_dict ={
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'District': districts['entityName'],
                                    'Transaction_Type': districts['metric']['type'],
                                    'Transaction_count': districts['metric']['count'],
                                    'Transaction_Amount': districts['metric']['amount']
                                    }
                                data_list.append(row_dict)

#Convert list fo dictonaries to dataframe
df5 = pd.DataFrame(data_list)


#<--------------------------$$$------------------------->

#for top=>users
# Inittialize empty list for holding the files
data_list = []
root_dir = (r"C:\Users\Akash\Desktop\ProjectPhonePePulse\pulse\data")
state_dir_path = os.path.join(root_dir, "top", "user", "country", "india", "state")
for state_dir in os.listdir(state_dir_path):
    state_path = os.path.join(state_dir_path, state_dir)
    if os.path.isdir(state_path):
        #iterate over all year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path,year_dir)
            if os.path.isdir(year_path):
                #iterate over all the json files for each quater respectively
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path,json_file)) as f:
                            data = json.load(f)
                            #Extract our data
                            for district in data['data']['districts']:
                                row_dict ={
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'RegisteredUsers': district['registeredUsers'],
                                    'District': district['name']
                                    if 'name' in district 
                                    else district['pincode'],               
                                    }
                                data_list.append(row_dict)

#Convert list fo dictonaries to dataframe
df6 = pd.DataFrame(data_list)


#<--------------------------$$$------------------------->

# Cleaning and transformation of data with help of pandas
#duplicates analyse and drop

d1 = df1.drop_duplicates()
d2 = df2.drop_duplicates()
d3 = df3.drop_duplicates()
d4 = df4.drop_duplicates()
d5 = df5.drop_duplicates()
d6 = df6.drop_duplicates()

null_counts = d1.isnull().sum()
null_counts = d2.isnull().sum()
null_counts = d3.isnull().sum()
null_counts = d4.isnull().sum()
null_counts = d5.isnull().sum()
null_counts = d6.isnull().sum()

# Renaming the state names for choropleth execution
states_new_name = {
    'andaman-&-nicobar-islands':'Andaman & Nicobar',
    'andhra-pradesh':'Andhra Pradesh',
    'arunachal-pradesh':'Arunachal Pradesh',
    'assam':'Assam',
    'bihar':'Bihar',
    'chandigarh':'Chandigarh',
    'chhattisgarh':'Chhattisgarh',
    'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
    'delhi':'Delhi',
    'goa':'Goa',
    'gujarat':'Gujarat',
    'haryana':'Haryana',
    'himachal-pradesh':'Himachal Pradesh',
    'jammu-&-kashmir':'Jammu & Kashmir',
    'jharkhand':'Jharkhand',
    'karnataka':'Karnataka',
    'kerala':'Kerala',
    'ladakh':'Ladakh',
    'lakshadweep':'Lakshadweep',
    'madhya-pradesh':'Madhya Pradesh',
    'maharashtra':'Maharashtra',
    'manipur':'Manipur',
    'meghalaya':'Meghalaya',
    'mizoram':'Mizoram',
    'nagaland':'Nagaland',
    'odisha':'Odisha',
    'puducherry':'Puducherry',
    'punjab':'punjab',
    'rajasthan':'Rajasthan',
    'sikkim':'Sikkim',
    'tamil-nadu':'Tamil Nadu',
    'telangana':'Telangana',
    'tripura':'Tripura',
    'uttarakhand':'Uttarakhand',
    'uttar-pradesh':'Uttar Pradesh',
    'west-bengal':'West Bengal'
}
d1["States"] = d1["States"].map(states_new_name)



################# create and insert sql database for dataframe1 ##############

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="Phone_Pe",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# SQL query to create the table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS aggregated_transaction (
    States VARCHAR(255),
    Transaction_Year INTEGER,
    Quarters INTEGER,
    Transaction_Type VARCHAR(255),
    Transaction_Count INTEGER,
    Transaction_Amount NUMERIC
);
"""
cur.execute(create_table_query)
conn.commit()

# Prepare the data for insertion (assuming d1 is your DataFrame)
data_d1 = d1[["States", "Transaction_Year", "Quarter", "Transaction_Type", "Transaction_Count", "Transaction_Amount"]].values.tolist()

# SQL query to insert data into the table
insert_data_query = """
INSERT INTO aggregated_transaction (States, Transaction_Year, Quarters, Transaction_Type, Transaction_Count, Transaction_Amount)
VALUES (%s, %s, %s, %s, %s, %s);
"""
cur.executemany(insert_data_query, data_d1)
conn.commit()
cur.close()
conn.close()
print("aggregated_transaction created and data inserted successfully in SQL!")

################# create and insert sql database for dataframe2 ##############

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="Phone_Pe",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# SQL query to create the table if it does not exist
create_table_query_d2 = """
CREATE TABLE IF NOT EXISTS aggregated_user (
    States VARCHAR(255),
    Transaction_Year INTEGER,
    Quarters INTEGER,
    RegisteredUsers BIGINT,
    Brand VARCHAR(255),
    Count BIGINT
);
"""
cur.execute(create_table_query_d2)
conn.commit()

# Prepare the data for insertion (assuming d2 is your DataFrame)
data_d2 = d2[["States", "Transaction_Year", "Quarter", "RegisteredUsers", "Brand", "Count"]].values.tolist()

# SQL query to insert data into the table
insert_data_query_d2 = """
INSERT INTO aggregated_user (States, Transaction_Year, Quarters, RegisteredUsers, Brand, Count)
VALUES (%s, %s, %s, %s, %s, %s);
"""
cur.executemany(insert_data_query_d2, data_d2)
conn.commit()
cur.close()
conn.close()
print("aggregated_user created and data inserted successfully in SQL!")

################# create and insert sql database for dataframe3 ##############

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="Phone_Pe",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
)
# Create a cursor object to execute SQL queries
cur = conn.cursor()

# SQL query to create the table if it does not exist
create_table_query_d3 = """
CREATE TABLE IF NOT EXISTS map_transactions (
    States VARCHAR(255),
    Transaction_Year INTEGER,
    Quarters INTEGER,
    District VARCHAR(255),
    Transaction_Type VARCHAR(255),
    Transaction_Count BIGINT
);
"""
cur.execute(create_table_query_d3)
conn.commit()

# Prepare the data for insertion (assuming d3 is your DataFrame)
data_d3 = d3[["States", "Transaction_Year", "Quarter", "District", "Transaction_Type", "Transaction_Count"]].values.tolist()

# SQL query to insert data into the table
insert_data_query_d3 = """
INSERT INTO map_transactions (States, Transaction_Year, Quarters, District, Transaction_Type, Transaction_Count)
VALUES (%s, %s, %s, %s, %s, %s);
"""
cur.executemany(insert_data_query_d3, data_d3)
conn.commit()
cur.close()
conn.close()
print("map_transactions created and data inserted successfully in SQL!")

################# create and insert sql database for dataframe4 ##############

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="Phone_Pe",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
)
# Create a cursor object to execute SQL queries
cur = conn.cursor()

# SQL query to create the table if it does not exist
create_table_query_d4 = """
CREATE TABLE IF NOT EXISTS map_user (
    States VARCHAR(255),
    Transaction_Year INTEGER,
    Quarters INTEGER,
    District VARCHAR(255),
    RegisteredUsers BIGINT
);
"""
cur.execute(create_table_query_d4)
conn.commit()

# Prepare the data for insertion (assuming d4 is your DataFrame)
data_d4 = d4[["States", "Transaction_Year", "Quarter", "District", "RegisteredUsers"]].values.tolist()

# SQL query to insert data into the table
insert_data_query_d4 = """
INSERT INTO map_user (States, Transaction_Year, Quarters, District, RegisteredUsers)
VALUES (%s, %s, %s, %s, %s);
"""
cur.executemany(insert_data_query_d4, data_d4)
conn.commit()
cur.close()
conn.close()
print("map_user created and data inserted successfully in SQL!")

################# create and insert sql database for dataframe5 ##############

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="Phone_Pe",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
)
# Create a cursor object to execute SQL queries
cur = conn.cursor()

# SQL query to create the table if it does not exist
create_table_query_d5 = """
CREATE TABLE IF NOT EXISTS top_transaction (
    States VARCHAR(255),
    Transaction_Year INTEGER,
    Quarters INTEGER,
    District VARCHAR(255),
    Transaction_Type VARCHAR(255),
    Transaction_count INTEGER,
    Transaction_Amount NUMERIC
);
"""
cur.execute(create_table_query_d5)
conn.commit()

# Prepare the data for insertion (assuming d5 is your DataFrame)
data_d5 = d5[["States", "Transaction_Year", "Quarter", "District", "Transaction_Type", "Transaction_count", "Transaction_Amount"]].values.tolist()

# SQL query to insert data into the table
insert_data_query_d5 = """
INSERT INTO top_transaction (States, Transaction_Year, Quarters, District, Transaction_Type, Transaction_count, Transaction_Amount)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
cur.executemany(insert_data_query_d5, data_d5)
conn.commit()
cur.close()
conn.close()
print("top_transaction created and data inserted successfully in SQL!")

################# create and insert sql database for dataframe6 ##############

# Establish a connection to PostgreSQL
conn = psycopg2.connect(
    dbname="Phone_Pe",
    user="postgres",
    password="dinhata",
    host="localhost",
    port="5432"
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# SQL query to create the table if it does not exist
create_table_query_d6 = """
CREATE TABLE IF NOT EXISTS top_user (
    States VARCHAR(255),
    Transaction_Year INTEGER,
    Quarters INTEGER,
    RegisteredUsers INTEGER,
    District VARCHAR(255)
);
"""
cur.execute(create_table_query_d6)
conn.commit()

# Prepare the data for insertion (assuming d6 is your DataFrame)
data_d6 = d6[["States", "Transaction_Year", "Quarter", "RegisteredUsers", "District"]].values.tolist()

# SQL query to insert data into the table
insert_data_query_d6 = """
INSERT INTO top_user (States, Transaction_Year, Quarters, RegisteredUsers, District)
VALUES (%s, %s, %s, %s, %s);
"""
cur.executemany(insert_data_query_d6, data_d6)
conn.commit()
cur.close()
conn.close()
print("top_user created and data inserted successfully in SQL!")

##------------------------@@@@@@@@@@@@@@@@@@------------------------##

# NOW SINCE THE DATA IS STORE IN A RELATIVE DATABASE (SQL), WE CAN BUILS A CODE TO VIEW EVERYHTING TO USER IN STREAMLIT APP 
# Refer PhonePeStreamlitMain.py file for execution