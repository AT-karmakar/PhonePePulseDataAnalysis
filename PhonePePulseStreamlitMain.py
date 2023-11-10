# streamlit run c:/Users/Akash/Desktop/ProjectPhonePePulse/PhonePePulseStreamlitMain.py

import git
import pandas as pd
import json
import csv
import os
import psycopg2
import streamlit as st
import PIL
from PIL import Image
from streamlit_option_menu import option_menu
import plotly.express as px
import altair as alt

# Note: Before running this file, run phonepe_data_extraction_&_sql.py file for extracting data from repository and inserting into SQL
#######------------------------%%%%%%%%%%%-------------------###########
#------------ STREAMLIT BUILDING CODE

st.set_page_config(
    page_title="Phone_Pe Pulse data",
    page_icon="📲",
    layout="wide",
    initial_sidebar_state="auto",
)

# Set the background color to white using custom CSS styles
st.markdown(
    """
    <style>
    body {
        background-color: #f6f5f7; /* Set the background color to white (#ffffff) */
    }
    </style>
    """,
    unsafe_allow_html=True
)
css = """
    <style>
    .stApp {
        border: 5px solid #6739b7;
    }
    </style>
"""

# Use the st.markdown() function to inject the CSS string into the Streamlit app
st.markdown(css, unsafe_allow_html=True)


SELECT = option_menu(
    menu_title = None,
    options = ["Home","Basic Insights","About"],
    icons = ['house','toggles','bar-chart'],
    default_index=0,
    orientation = 'horizontal',
    styles = {'container':{'padding': '0!important','background-color':'off-white','size':'cover','width':'100%'},
              'icon': {'color': 'black','font-size':'20px'},
              'nav-link': {'font-size': '20px','text-align':'center','margin':'-1px','color':'black'},
              'nav-link-selected':{'background-color':' #6739b7','color':'white'}}
)

if SELECT == 'Basic Insights':
    col1,col2 = st.columns(2, gap = 'small')
    col1.markdown("<h1 style='color: #6739b7;'>BASIC INSIGHTS</h1>", unsafe_allow_html=True)
    col1.caption("""Explore fundamental insights and key metrics derived from PhonePe Pulse data. Gain a comprehensive overview of transaction trends, user engagement, and regional analysis.
                 This section provides a high-level understanding of the PhonePe ecosystem, including transaction counts, user engagement metrics, and regional transaction patterns.
                 Dive into the essential data points to grasp the pulse of PhonePe's progress.""")
    
    # Open the image using PIL
    original_image = Image.open(r'C:\Users\Akash\Downloads\pulseINFO.png')

    # Resize the image to your desired height
    desired_height = 2000  # Replace this with the height you want
    resized_image = original_image.resize((int(original_image.width * (desired_height / original_image.height)), desired_height))
    col2.image(resized_image, width=750)

    #col2.image(Image.open(r'C:\Users\Akash\Downloads\pulseINFO.png'), width = 1000)
    st.write('-------')
    st.subheader('Some basic insights about the data')
    options = ['--Select--',
               'Top states by Transaction Amount',
               'Top 10 Popular Transaction Types',
               'Transaction Trends Over Quarters',
               'User Engagement',
               'Districts with High Transaction Counts',
               'Transaction Types vs. Transaction Amount',
               'Top Districts by Transaction Amount',
               'Transaction Count vs. District Analysis',
               'User Engagement by State',
               'Quarterly Growth Analysis'
               ]
    
    select = st.selectbox('Select the option',options)
  
    
###############################################################################

    if select=='--Select--':
        st.caption("Please select any option to get insights from the data")
        st.write('-------')
        st.markdown(
        """
        <div style='text-align: center; font-style: italic; font-size: 12px; color: gray; margin-top: 20px;'>
            Tecchnologies used: Github Cloning, Python, Pandas, MySQL,mysql-connector-python, Streamlit, and Plotly.
        </div>
        """,
        unsafe_allow_html=True
        )
#########################################################################

    elif select=='Top states by Transaction Amount':
        with st.spinner('Loading Please Wait...'):

        # Function to fetch data from the database and identify top states by transaction amount
            def identify_top_states_by_transaction_amount(selected_year):
                # Establish a connection to PostgreSQL
                conn = psycopg2.connect(
                    dbname="Phone_Pe",
                    user="postgres",
                    password="yourpassword",
                    host="localhost",
                    port="5432"
                )
                # SQL query to retrieve data from the aggregated_transaction table for the selected year
                transaction_query = f"""
                SELECT States, SUM(Transaction_Amount) AS TotalTransactionAmount
                FROM aggregated_transaction
                WHERE Transaction_Year = {selected_year}
                GROUP BY States
                ORDER BY TotalTransactionAmount DESC;
                """
                # Execute the query and fetch the results into a DataFrame
                transaction_df = pd.read_sql(transaction_query, conn)

                # Close the database connection
                conn.close()

                return transaction_df

            # Streamlit app for Top States by Transaction Amount analysis
            st.title('Top States by Transaction Amount')
            st.write('Identify the top states with the highest total transaction amounts. This helps in understanding the regions contributing the most to the transactions.')
            
            # Year selection slider
            selected_year = st.slider('Select Year', min_value=2018, max_value=2023, value=2023, step=1)

            # Call the function to fetch data and identify top states by transaction amount for the selected year
            top_states_df = identify_top_states_by_transaction_amount(selected_year)

            # Display the DataFrame with top states by transaction amount
            st.subheader('Top States by Transaction Amount')
            st.write(top_states_df)

            # Visualize the top states by transaction amount using Plotly bar chart
            fig_top_states_transaction_amount = px.bar(top_states_df, x='states', y='totaltransactionamount',
                                                        labels={'totaltransactionamount': 'Total Transaction Amount'},
                                                        title=f'Top States by Transaction Amount for {selected_year}')
            
            fig_top_states_transaction_amount.update_layout(
            width=1300,  # Set custom width
            height=700  # Set custom height
            )

            st.plotly_chart(fig_top_states_transaction_amount)

    ######################################################################

    elif select == 'Top 10 Popular Transaction Types':
        with st.spinner('Loading Please Wait...'):
            st.write( 'Determine the most common transaction types across different states and quarters. This insight helps in understanding the preferred payment methods.')
        # Year selection slider
            selected_year = st.slider('Select Year', min_value=2018, max_value=2023, value=2023, step=1)

            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            # SQL query to retrieve popular transaction types for the selected year
            query = f"""
            SELECT States, Transaction_Type, SUM(Transaction_Count) AS Total_Transactions
            FROM aggregated_transaction
            WHERE Transaction_Year = {selected_year}
            GROUP BY States, Transaction_Type
            ORDER BY Total_Transactions DESC
            LIMIT 10;
            """
            # Execute the query and fetch data into a DataFrame
            df_popular_transactions = pd.read_sql_query(query, conn)

            # Convert column names to lowercase
            df_popular_transactions.columns = map(str.lower, df_popular_transactions.columns)
            conn.close()

            # Visualize the data using a bar chart
            fig = px.bar(
                df_popular_transactions,
                x='transaction_type',
                y='total_transactions',
                color='states',
                title=f'Top 10 Popular Transaction Types Across States and Quarters for {selected_year}',
                labels={'total_transactions': 'Total Transactions'},
                barmode='group',
            )
            fig.update_layout(
            width=1200,  # Set custom width
            height=600  # Set custom height
            )
            # Show the plot
            st.plotly_chart(fig)

    #####################################################################

    elif select=='Transaction Trends Over Quarters':
        with st.spinner('Loading Please Wait...'):

            st.write('Analyze how transaction counts and amounts vary across quarters to identify any seasonal trends or changes in user behavior.')
            # SQL query to retrieve transaction trends over quarters
            query_transaction_trends = """
            SELECT Transaction_Year, Quarters, SUM(Transaction_Count) AS Total_Transaction_Count, SUM(Transaction_Amount) AS Total_Transaction_Amount
            FROM aggregated_transaction
            GROUP BY Transaction_Year, Quarters
            ORDER BY Transaction_Year, Quarters;
            """
            # Establish a connection to PostgreSQL
            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            df_transaction_trends = pd.read_sql_query(query_transaction_trends, conn)
            conn.close()
            df_transaction_trends.columns = map(str.lower, df_transaction_trends.columns)

            # Plotting the Transaction Trends Over Quarters
            st.title("Transaction Trends Over Quarters")
            st.subheader("Analyze how transaction counts and amounts vary across quarters")
            df_transaction_trends
            # Visualize the data using line charts
            fig1 = px.line(
                df_transaction_trends,
                x='quarters',
                y='total_transaction_count',
                color='transaction_year',
                labels={'total_transaction_count': 'Total Transaction Count'},
                title='Transaction Counts Over Quarters',
                markers=True
            )
            fig2 = px.line(
                df_transaction_trends,
                x='quarters',
                y='total_transaction_amount',
                color='transaction_year',
                labels={'total_transaction_amount': 'Total Transaction Amount'},
                title='Transaction Amounts Over Quarters',
                markers=True
            )
            fig1.update_layout(
            width=1200,  
            height=600 
            )
            fig2.update_layout(
            width=1200,  
            height=600 
            )
            # Display the line charts in Streamlit app
            st.plotly_chart(fig1)
            st.plotly_chart(fig2)

    ###################################################################
    
    elif select == 'User Engagement':
        with st.spinner('Loading Please Wait...'):
            st.write('Explore the districts with the highest number of registered users. Understanding user engagement at the district level can aid in targeted marketing strategies.')
            # Year selection dropdown
            selected_year = st.selectbox('Select Year', [2018, 2019, 2020, 2021, 2022, 2023])

            # SQL query to retrieve user engagement data for the selected year
            query_user_engagement = f"""
                SELECT States, District, SUM(RegisteredUsers) AS Total_Registered_Users
                FROM top_user
                WHERE Transaction_Year = {selected_year}
                GROUP BY States, District
                ORDER BY Total_Registered_Users DESC
                LIMIT 10;
            """
            # Establish a connection to PostgreSQL
            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            # Execute the query and fetch data into a DataFrame
            df_user_engagement = pd.read_sql_query(query_user_engagement, conn)
            conn.close()

            # Convert column names to lowercase
            df_user_engagement.columns = map(str.lower, df_user_engagement.columns)

            # Combine state and district names for the sunburst chart labels
            df_user_engagement['location'] = df_user_engagement['states'] + ' - ' + df_user_engagement['district']

            # Plotting the User Engagement data using a sunburst chart
            fig_user_engagement = px.sunburst(
                df_user_engagement,
                path=['location'],
                values='total_registered_users',
                title=f'Distribution of Registered Users in Districts for the year {selected_year}',
                width=800,
                height=800,
                color_discrete_sequence=px.colors.qualitative.Set3  # Stylish color scheme
            )
            # Display the sunburst chart in Streamlit app
            st.plotly_chart(fig_user_engagement)

    #####################################################################

    elif select == 'Districts with High Transaction Counts':
        with st.spinner('Loading Please Wait...'):
            st.write('Identify districts with the highest transaction counts. This information can be valuable for businesses looking to expand or focus their services in specific regions.')
            # SQL query to retrieve districts with high transaction counts for the selected year
            selected_year = st.selectbox('Select Year', [2018, 2019, 2020, 2021, 2022, 2023], index=5)

            query_transaction_counts = f"""
            SELECT States, District, SUM(Transaction_Count) AS Total_Transaction_Count
            FROM map_transactions
            WHERE Transaction_Year = {selected_year}
            GROUP BY States, District
            ORDER BY Total_Transaction_Count DESC
            LIMIT 10;
            """
            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            
            # Execute the query and fetch data into a DataFrame
            df_transaction_counts = pd.read_sql_query(query_transaction_counts, conn)

            # Convert column names to lowercase
            df_transaction_counts.columns = map(str.lower, df_transaction_counts.columns)

            # Create a new column combining state and district names
            df_transaction_counts['location'] = df_transaction_counts['states'] + ' - ' + df_transaction_counts['district']

            # Visualize the data using a donut chart
            fig_transaction_counts_donut = px.sunburst(
            df_transaction_counts,
            path=['states', 'district'],
            values='total_transaction_count',
            title=f'Districts with High Transaction Counts for {selected_year}',
            width=1000,
            height=800,
            color_discrete_sequence=px.colors.qualitative.Set1
            )
            st.plotly_chart(fig_transaction_counts_donut)

        ####################################################################
    
    elif select == 'Transaction Types vs. Transaction Amount': 
        with st.spinner('Loading Please Wait...'):
            st.write('Investigate the relationship between transaction types and their corresponding amounts. Determine which transaction types tend to have higher transaction amounts.')
            # Multi-select for years
            selected_years = st.multiselect('Select Years', [2018, 2019, 2020, 2021, 2022, 2023], default=[2023])

            # SQL query to retrieve transaction types, their corresponding amounts, and years
            selected_years_str = ', '.join(map(str, selected_years))
            query_transaction_amounts = f"""
            SELECT Transaction_Type, SUM(Transaction_Amount) AS Total_Transaction_Amount, Transaction_Year
            FROM aggregated_transaction
            WHERE Transaction_Year IN ({selected_years_str})
            GROUP BY Transaction_Type,Transaction_Year
            ORDER BY Transaction_Type, Transaction_Year;
            """
            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            # Execute the query and fetch data into a DataFrame
            df_transaction_amounts = pd.read_sql_query(query_transaction_amounts, conn)
            conn.close()

            # Convert column names to lowercase
            df_transaction_amounts.columns = map(str.lower, df_transaction_amounts.columns)

            # Define a custom color palette
            colors = px.colors.qualitative.Set2

            # Visualize the data using a pie chart
            fig_transaction_amounts_pie = px.pie(
                df_transaction_amounts,
                names='transaction_type',
                values='total_transaction_amount',
                title=f'Transaction Types Distribution for {", ".join(map(str, selected_years))}',
                width=1000,
                height=800,
                color_discrete_sequence=px.colors.qualitative.Set2
                )
            st.plotly_chart(fig_transaction_amounts_pie)
    
        ########################################################################
    
    elif select=='Top Districts by Transaction Amount':
        with st.spinner('Loading Please Wait...'):
            st.write('Identify districts with the highest transaction amounts. This insight is valuable for businesses to identify lucrative markets for their services.')
            # Streamlit app title and year selection
            st.title("Top Districts by Transaction Amount")
            
            selected_year = st.slider('Select Year', min_value=2018, max_value=2023, value=2023, step=1)

            # SQL query to retrieve top districts by transaction amount for the selected year
            query_top_districts_transaction_amount = f"""
                SELECT District, States, SUM(Transaction_Amount) AS Total_Transaction_Amount
                FROM top_transaction
                WHERE Transaction_Year = {selected_year}
                GROUP BY District, States
                ORDER BY Total_Transaction_Amount DESC
                LIMIT 10;
            """

            # Establish a connection to PostgreSQL
            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            # Execute the query and fetch data into a DataFrame
            df_top_districts_transaction_amount = pd.read_sql_query(query_top_districts_transaction_amount, conn)
            conn.close()

            # Convert column names to lowercase
            df_top_districts_transaction_amount.columns = map(str.lower, df_top_districts_transaction_amount.columns)

            # Create a horizontal bar chart with state and district names
            fig = px.bar(df_top_districts_transaction_amount, x='total_transaction_amount', y='district',
                        text='states',  color = 'states',
                        labels={'total_transaction_amount': 'Total Transaction Amount'},
                        title=f'Top Districts by Transaction Amount for {selected_year}')

            # Customize the layout for better readability
            fig.update_traces(textposition='inside')  # Position the state names inside the bars for clarity
            fig.update_layout(
                xaxis_title='Total Transaction Amount',
                yaxis_title='Districts',
                width=900,  
                height=700,  
                margin=dict(l=20, r=20, t=40, b=40)       # Set margins for better spacing
            )
            st.plotly_chart(fig)

        ##################################################################

    elif select=='Transaction Count vs. District Analysis':
        with st.spinner('Loading Please Wait...'):
            st.write('Analyze transaction counts across different districts. Identify districts with consistently high transaction counts, indicating strong market demand.')
            # Function to fetch data from the database and analyze transaction counts by district
            def analyze_transaction_counts_by_district():
                # Establish a connection to PostgreSQL
                conn = psycopg2.connect(
                    dbname="Phone_Pe",
                    user="postgres",
                    password="yourpassword",
                    host="localhost",
                    port="5432"
                )
                # SQL query to retrieve data from the aggregated_transaction table
                transaction_query = """
                SELECT States, Transaction_Year, Quarters, District, SUM(Transaction_Count) AS TotalTransactionCount
                FROM top_transaction
                GROUP BY States, Transaction_Year, Quarters, District
                ORDER BY States, Transaction_Year, Quarters, District;
                """

                # Execute the query and fetch the results into a DataFrame
                transaction_df = pd.read_sql(transaction_query, conn)
                conn.close()

                return transaction_df

            # Streamlit app for Transaction Count vs. District analysis
            st.title('Transaction Count vs. District Analysis')
            st.write('Analyzing transaction counts across different districts.')

            # Call the function to fetch data and analyze transaction counts by district
            transaction_counts_df = analyze_transaction_counts_by_district()

            # Display the DataFrame with transaction counts by district
            st.subheader('Transaction Counts by District')
            st.write(transaction_counts_df)

            # Visualize the transaction counts using Plotly chart
            selected_state = st.selectbox('Select State:', transaction_counts_df['states'].unique())
            selected_districts = transaction_counts_df[transaction_counts_df['states'] == selected_state]['district'].unique()
            selected_district = st.selectbox('Select District:', selected_districts)

            filtered_df = transaction_counts_df[(transaction_counts_df['states'] == selected_state) & 
                                                (transaction_counts_df['district'] == selected_district)]


            fig_transaction_counts = px.bar(filtered_df, x='quarters', y='totaltransactioncount',
                                            labels={'totaltransactioncount': 'Total Transaction Count','quarters': 'Quarters'},
                                            title=f'Transaction Counts in {selected_district}, {selected_state}',
                                            color='quarters', height = 690, width= 800,
                                            color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig_transaction_counts)
    
    #############################################################################

    elif select=='User Engagement by State':
        with st.spinner('Loading Please Wait...'):
            st.write('Compare user engagement (registered users) across different states. Understand which states have a higher proportion of registered users, indicating user interest and adoption.')
            # Establish a connection to PostgreSQL
            conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
            # SQL query to retrieve data from the aggregated_user table
            user_engagement_query = """
            SELECT States, SUM(RegisteredUsers) AS TotalRegisteredUsers
            FROM aggregated_user
            GROUP BY States
            ORDER BY TotalRegisteredUsers DESC;
            """
            # Execute the query and fetch the results into a DataFrame
            user_engagement_df = pd.read_sql(user_engagement_query, conn)

            # Close the database connection
            conn.close()

            # Create a Plotly bar chart
            fig = px.bar(user_engagement_df, x='states', y='totalregisteredusers',
                        labels={'total_registered_users': 'Total Registered Users'},
                        title='User Engagement by State',color='states',
                        color_discrete_sequence=px.colors.qualitative.Set3
                        )
            
            fig.update_layout(
            width=1500,  # Set custom width
            height=900  # Set custom height
            )

            # Streamlit app
            st.title('User Engagement Analysis')
            st.write('Compare user engagement (registered users) across different states.')
            st.plotly_chart(fig)

    ###################################################################

    elif select=='Quarterly Growth Analysis':
        with st.spinner('Loading Please Wait...'):
            st.write('Calculate the quarter-to-quarter growth rates for transaction counts and amounts. Identify quarters with significant growth or decline, providing insights into user behavior changes over time.')
            # Function to fetch data from the database and calculate growth rates
            def calculate_quarterly_growth():
                # Establish a connection to PostgreSQL
                conn = psycopg2.connect(
                    dbname="Phone_Pe",
                    user="postgres",
                    password="yourpassword",
                    host="localhost",
                    port="5432"
                )
                # SQL query to retrieve data from the aggregated_transaction table
                transaction_query = """
                SELECT States, Transaction_Year, Quarters, SUM(Transaction_Count) AS TotalTransactionCount, SUM(Transaction_Amount) AS TotalTransactionAmount
                FROM aggregated_transaction
                GROUP BY States, Transaction_Year, Quarters
                ORDER BY States, Transaction_Year, Quarters;
                """
                # Execute the query and fetch the results into a DataFrame
                transaction_df = pd.read_sql(transaction_query, conn)
                conn.close()
                
                # Calculate quarter-to-quarter growth rates for transaction counts and amounts
                transaction_df['TransactionCountGrowth'] = transaction_df.groupby('states')['totaltransactioncount'].pct_change() * 100
                transaction_df['TransactionAmountGrowth'] = transaction_df.groupby('states')['totaltransactionamount'].pct_change() * 100

                return transaction_df

            # Streamlit app
            st.title('Quarterly Growth Analysis')
            st.write('Calculate the quarter-to-quarter growth rates for transaction counts and amounts.')

            # Call the function to fetch data and calculate growth rates
            quarterly_growth_df = calculate_quarterly_growth()

            # Display the DataFrame with growth rates
            st.subheader('Quarterly Growth Rates for Transaction Counts and Amounts')
            st.write(quarterly_growth_df)
            selected_state = st.selectbox('Select State:', quarterly_growth_df['states'].unique())
            filtered_df = quarterly_growth_df[quarterly_growth_df['states'] == selected_state]

            fig_count_growth = px.box(filtered_df, x='quarters', y='TransactionCountGrowth',
                                    labels={'TransactionCountGrowth': 'Transaction Count Growth (%)'}, 
                                    height=500, width=700,
                                    title=f'Transaction Count Growth Rate for {selected_state}')
            st.plotly_chart(fig_count_growth)

            fig_amount_growth = px.box(filtered_df, x='quarters', y='TransactionAmountGrowth',
                                        labels={'TransactionAmountGrowth': 'Transaction Amount Growth (%)'},
                                        height=500, width=700,
                                        title=f'Transaction Amount Growth Rate for {selected_state}')
            st.plotly_chart(fig_amount_growth)

##################################################################

#geo visualisation part by choropleath 
#######################################################################    

if SELECT == 'Home':
    col1, col2 = st.columns(2)
    #col1.image(Image.open(r'C:\Users\Akash\Downloads\PhonePe-Logo.png'), width = 500)
    with col1:
        st.image(Image.open(r'C:\Users\Akash\Downloads\PhonePe-Logo.png'), width = 300)
        st.subheader("PhonePe is an Indian digital payments and financial services company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer.")
        st.download_button("DOWNLOAD THE APP",r'https://www.phonepe.com/app-download' )
    with col2:
        st.video(r'C:\Users\Akash\Downloads\phone_pe_vid.mp4')
    
    col1.image(Image.open(r'C:\Users\Akash\Downloads\decoding-bcg-report.png'), width = 700)

    conn = psycopg2.connect(
                dbname="Phone_Pe",
                user="postgres",
                password="yourpassword",
                host="localhost",
                port="5432"
            )
    cur = conn.cursor()

    # Slider for selecting the year (2018 to 2023)
    selected_year = st.slider('Select Year', min_value=2018, max_value=2023, step=1, value=2023)

    # Slider for selecting quarters (1 to 4)
    selected_quarter = st.slider('Select Quarter', min_value=1, max_value=4, step=1, value=1)
    # Execute the query and fetch data into a DataFrame
    query = f"""
        SELECT States, Transaction_Year, Quarters, Transaction_Type, Transaction_Count, Transaction_Amount
        FROM aggregated_transaction
        WHERE Transaction_Year = {selected_year} AND Quarters = {selected_quarter}
    """
    cur.execute(query)
    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns = ['States','Transaction_Year','Quarters','Transaction_Type','Transaction_Count','Transaction_Amount'])
    fig = px.choropleth(df, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey = 'properties.ST_NM',
                        locations = 'States', color = 'States' ,color_continuous_scale= 'Virdis', hover_data=['Transaction_Amount','Transaction_Count']) 
    
    fig.update_geos(fitbounds="locations", visible=True)
    fig.update_layout(
        title=dict(
        text='Live Geo Visualization of PhonePe Transactions in India',
        font=dict(size=40, color='#6739b7')  # Set the size and color of the title
        ),
        width=1300,  
        height=900  
        )
    st.plotly_chart(fig)

    st.markdown(
    """
    <div style='text-align: center; font-style: italic; font-size: 12px; color: gray; margin-top: 20px;'>
        Tecchnologies used: Github Cloning, Python, Pandas, MySQL,mysql-connector-python, Streamlit, and Plotly.
    </div>
    """,
    unsafe_allow_html=True
    )

#################################################################################################################

if SELECT == 'About':
    col1,col2 = st.columns(2)
    with col1:
        st.video(r'C:\Users\Akash\Downloads\IntroPhonePePulse.mp4')
    with col2:
        st.image(Image.open(r'C:\Users\Akash\Downloads\upi-apps-market-share-by-transaction-value-StartupTalky-1.jpg'),width = 700)
        st.write("-------------")
    st.subheader("The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016. The PhonePe app is available in 11 Indian languages."
                    "Using PhonePe, users can send and receive money, recharge mobile, DTH, data cards, make utility payments, pay at shops, invest in tax saving funds, buy insurance, mutual funds, and digital gold."
                    "PhonePe is accepted as a payment option by over 3.5 crore offline and online merchant outlets, constituting 99% of pin codes in the country."
                    "The app served more than 10 crore users as of June 2018, processed 500 crore transactions by December 2019, and crossed 10 crore transactions a day in April 2022. It currently has over 44 crore registered users with over 20 crore monthly active users."
                    "PhonePe is licensed by the Reserve Bank of India for the issuance and operation of a Semi Closed Prepaid Payment system.")

    st.write("------")
    col1,col2 = st.columns(2)
    with col1:
        st.markdown("<h1 style='color: #6739b7;'>THE BEAT OF PROGRESS</h1>", unsafe_allow_html=True)
        st.write('---------')
        st.subheader('Phonepe is a leading digital payments company')
        st.image(Image.open(r'C:\Users\Akash\Downloads\best-payment-app-in-india.png'), width =650)
        with open(r'C:\Users\Akash\Downloads\AnnualReport.pdf','rb') as f:
            data = f.read()
        st.download_button('DOWNLOAD REPORT', data,file_name='AnnualReport.pdf')
    with col2:
        st.image(Image.open(r'C:\Users\Akash\Downloads\PhonePe_Pulse.jpg'), width = 600)
    st.markdown(
    """
    <div style='text-align: center; font-style: italic; font-size: 12px; color: gray; margin-top: 20px;'>
        Tecchnologies used: Github Cloning, Python, Pandas, MySQL,mysql-connector-python, Streamlit, and Plotly.
    </div>
    """,
    unsafe_allow_html=True
    )
    st.markdown(
    """
    <div style='text-align: center; font-style: italic; font-size: 20px; color: gray; margin-top: 20px;'>
    Thank You for Viewing!
    </div>
    """,
    unsafe_allow_html=True
    )


