from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


dag = DAG('KPI',
          description='Returns list of kpi for overview of customer sales',
          schedule_interval='0 5 * * *',
          start_date=datetime(2024, 7, 10), catchup=False)

# Download the data from S3
s3_download_operator = BashOperator(task_id='s3_download',
                                    bash_command='C:/Users/18572/material/scripts/s3_download.py', ##<<<< edit path!!
                                    dag=dag)

dealyed_orders_job_operator = BashOperator(task_id='dealyed_orders_job',
                                              bash_command=r'C:\\Users\18572\\material\scripts\dealyed_orders_job.py', ##<<<< edit path!!
                                              dag=dag)
dealyed_orders_job_operator.set_upstream(s3_download_operator)

avg_delivery_time_operator = BashOperator(task_id='avg_deliver_job',
                                              bash_command=r'C:\\Users\18572\\material\scripts\avg_delivery_time_kpi.py', ##<<<< edit path!!
                                              dag=dag)

avg_delivery_time_operator.set_upstream(dealyed_orders_job_operator)

avg_shipping_time_operator = BashOperator(task_id='average_shipping_time_job',
                                              bash_command=r'C:\Users\18572\material\scripts\average_shipping_time(1).py', ##<<<< edit path!!
                                              dag=dag)

avg_shipping_time_operator.set_upstream(avg_delivery_time_operator)

calculate_supplier_lead_time_operator = BashOperator(task_id='calculate_supplier_lead_time_job',
                                              bash_command=r'C:\Users\18572\material\scripts\calculate_supplier_lead_time_kpi.py', ##<<<< edit path!!
                                              dag=dag)

calculate_supplier_lead_time_operator.set_upstream(avg_shipping_time_operator)

customer_churn_operator = BashOperator(task_id='customer_churn_job',
                                              bash_command=r'C:\Users\18572\material\scripts\customer_churn_kpi.py', 
                                              dag=dag)

customer_churn_operator.set_upstream(calculate_supplier_lead_time_operator)

customer_lifetime_operator = BashOperator(task_id='customer_lifetime_job',
                                              bash_command=r'C:\\Users\18572\\material\scripts\customer_lifetime_value.py', 
                                              dag=dag)

customer_lifetime_operator.set_upstream(customer_churn_operator)

# Upload cleaned dataset to S3
s3_upload_operator = BashOperator(task_id='s3_upload',
                                  bash_command=r'C:\\Users\18572\\material\scripts\s3_upload.py', 
                                  dag=dag)

# Specify that the S3 upload task depends on the Spark job running successfully
s3_upload_operator.set_upstream(customer_lifetime_operator)

s3_download_operator >> dealyed_orders_job_operator >> avg_delivery_time_operator >> avg_shipping_time_operator >> calculate_supplier_lead_time_operator >> customer_churn_operator >> customer_lifetime_operator >> s3_upload_operator
