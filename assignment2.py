import psycopg2 
from sqlalchemy import create_engine
import pandas as pd 
import matplotlib 
import sklearn 
from datetime import datetime
import time 
import matplotlib.pyplot as plt 
import matplotlib
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import csv

def report_extract():
    Day='01-01-2021'
    def Get_DF_i(Day):
        DF_i=None
        try: 
            URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day=pd.read_csv(URL_Day)
            DF_day['Day']=Day
            cond=(DF_day.Country_Region=='United Kingdom')&(DF_day.Province_State=='Bermuda')
            Selec_columns=['Day','Country_Region', 'Last_Update',
                'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio','Province_State']
            DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
        except:
        #print(f'{Day} is not available!')
            pass
        return DF_i

    List_of_days=[]
    for year in range(2021,2022):
        for month in range(1,13):
         for day in range(1,32):
            month=int(month)
            if day <=9:
                day=f'0{day}'

            if month <= 9 :
                month=f'0{month}'
            List_of_days.append(f'{month}-{day}-{year}')

    Start=time.time()
    DF_all=[]
    for Day in List_of_days:
        DF_all.append(Get_DF_i(Day))
    End=time.time()
    Time_in_sec=round((End-Start)/60,2)
    print(f'It took {Time_in_sec} minutes to get all data')
 
    DF_Unitedk=pd.concat(DF_all).reset_index(drop=True)
    DF_Unitedk['Last_Updat']=pd.to_datetime(DF_Unitedk.Last_Update, infer_datetime_format=True)  
    DF_Unitedk['Day']=pd.to_datetime(DF_Unitedk.Day, infer_datetime_format=True)  
    DF_Unitedk['Case_Fatality_Ratio']=DF_Unitedk['Case_Fatality_Ratio'].astype(float)
    DF_Unitedk.to_csv('/opt/airflow/dags/DF_Unitedk.csv')
    

def scaling_report():
    DF_Unitedk=pd.read_csv('/opt/airflow/dags/DF_Unitedk.csv')
    Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK_u_2=DF_Unitedk[Selec_Columns]
    min_max_scaler = MinMaxScaler()
    DF_UK_u_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_UK_u_2[Selec_Columns]),columns=Selec_Columns)
    DF_UK_u_3.index=DF_UK_u_2.index
    DF_UK_u_3['Day']=DF_Unitedk.Day
    DF_UK_u_3.to_csv('/opt/airflow/dags/DF_UK_u_3.csv')

def ploting_scaling():
    Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK_u_3=pd.read_csv('/opt/airflow/dags/DF_UK_u_3.csv')
    DF_UK_u_3[Selec_Columns].plot(figsize=(20,10))
    plt.savefig('/opt/airflow/dags/UK_scoring_report.png')

def scaling_to_csv():
    DF_UK_u_3=pd.read_csv('/opt/airflow/dags/DF_UK_u_3.csv')
    DF_UK_u_3.to_csv('/opt/airflow/dags/UK_scaled_scoring_report.csv')
    
def scaling_to_postgresql():
    DF_UK_u_3=pd.read_csv('/opt/airflow/dags/UK_scaled_scoring_report.csv')
    host="postgres" # use "localhost" if you access from outside the localnet docker-compose env 
    database="postgres"
    user="airflow"
    password="airflow"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    Day='01_01_2021'
    DF_UK_u_3.to_sql(f'UK_scoring_report_{Day}', engine,if_exists='replace',index=False)
    

##################################DAGS#############################

my_dag = DAG(
    dag_id = 'assignment2',
    #schedule_interval = "0 0 * * *",
    start_date=datetime(2021,5,23)
)

Create_Data = PythonOperator(task_id='Extract_report',
                             python_callable=report_extract,
                             dag=my_dag)

Scaling_reports = PythonOperator(task_id='Scaling_report',
                             python_callable=scaling_report,
                             dag=my_dag)                            

Plotings = PythonOperator(task_id='Ploting_Scaling_Report',
                             python_callable=ploting_scaling,
                             dag=my_dag)                            

to_csv = PythonOperator(task_id='Scaling_to_csv',
                             python_callable=scaling_to_csv,
                             dag=my_dag)                            

to_postgreSQL = PythonOperator(task_id='Scaling_to_postgreSQL',
                             python_callable=scaling_to_postgresql,
                             dag=my_dag)     

Create_Data >> Scaling_reports
Scaling_reports >> Plotings
Scaling_reports >> to_csv >> to_postgreSQL
