# PhonePePulseDataAnalysis
PhonePe Pulse Analytics Dashboard

>>Overview
PhonePe Pulse Analytics Dashboard is a data visualization tool that provides insights into transaction data collected by PhonePe, a leading digital payments platform. This interactive dashboard allows users to explore transaction trends, user engagement, and regional performance across different states and districts in India.

>>Features
*Transaction Trends: Analyze transaction counts, amounts, and types over different years.
*User Engagement: Visualize registered user counts in various states.
*Regional Performance: Explore transaction counts and amounts by districts within states.
*Interactive Visualizations: Use interactive charts to filter and explore data dynamically.
*Data Source: The dashboard fetches data from a PostgreSQL database containing aggregated transaction and user data.

>>Prerequisites
*Python 3.7+
*PostgreSQL database with relevant transaction and user data.

>>Setup
*Install dependencies: Run pip install -r requirements.txt to install required Python packages.
*Database Configuration: Set up the PostgreSQL database connection parameters in the code.
*Run the Application:1.Run the file phonepe_data_extraction_&_sql.py to extract the data and insert it into SQL database.
                      2.Execute streamlit run PhonePePulseStreamlitMain.py to start the Streamlit app.

>>Usage
*Upon running the application, open the provided URL in your browser to access the interactive dashboard.
*Select different options and filters to explore transaction data, user engagement, and regional performance.
