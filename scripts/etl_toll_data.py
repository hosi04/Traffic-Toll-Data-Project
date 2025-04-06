from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import tarfile
import csv

source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
destination_path = '/opt/airflow/dags/python_etl/staging'

# Function to download the dataset
def download_dataset():
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(f"{destination_path}/tolldata.tgz", "wb") as f:
            f.write(response.raw.read())
    else:
        print("Failed to download the file")

# Function to untar the dataset
def untar_dataset():
    with tarfile.open(f"{destination_path}/tolldata.tgz", "r:gz") as tar:
        tar.extractall(path=destination_path)

# Function to extract data from CSV
def extract_data_from_csv():
    input_file = f"{destination_path}/vehicle-data.csv"
    output_file = f"{destination_path}/csv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles'])
        for line in infile:
            row = line.split(',')
            writer.writerow([row[0], row[1], row[2], row[3], row[4]])

# Function to extract data from TSV
def extract_data_from_tsv():
    input_file = f"{destination_path}/tollplaza-data.tsv"
    output_file = f"{destination_path}/tsv_data.csv"
    
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        # Header
        writer.writerow(['Tollplaza id', 'Tollplaza code'])
        
        # Skip header row if your TSV file has one, otherwise remove next line
        next(infile, None)  # Comment this line if your TSV doesn't have a header
        
        # Process data rows
        for line in infile:
            row = line.strip().split('\t')
            # Extract columns: Number of axles (index 4), Tollplaza id (index 5), Tollplaza code (index 6)
            writer.writerow([row[5], f"{row[6]}"])  # Add quotes around Tollplaza code

# Function to extract data from fixed width file
def extract_data_from_fixed_width():
    input_file = f"{destination_path}/payment-data.txt"
    output_file = f"{destination_path}/fixed_width_data.csv"
    
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        # Write header
        writer.writerow(['Type of Payment code', 'Vehicle Code'])
        
        # Process each line
        for line in infile:
            # Lấy 9 ký tự cuối cùng của dòng (3 + 1 + 5)
            last_part = line.strip()[-9:]
            # Tách thành 2 cột: 3 ký tự đầu và 5 ký tự cuối
            payment_code = last_part[:3].strip()  # PTE, PTP, hoặc PTC
            vehicle_code = last_part[-5:].strip()  # VC965 hoặc VCD2F
            writer.writerow([payment_code, vehicle_code])

# Function to consolidate data
def consolidate_data():
    csv_file = f"{destination_path}/csv_data.csv"
    tsv_file = f"{destination_path}/tsv_data.csv"
    fixed_width_file = f"{destination_path}/fixed_width_data.csv"
    output_file = f"{destination_path}/extracted_data.csv"

    with open(csv_file, 'r') as csv_in, \
         open(tsv_file, 'r') as tsv_in, \
         open(fixed_width_file, 'r') as fixed_in, \
         open(output_file, 'w', newline='') as out_file:
        
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)

        # Write header
        writer.writerow([
            'Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type',
            'Number of axles', 'Tollplaza id', 'Tollplaza code',
            'Type of Payment code', 'Vehicle Code'
        ])

        # Skip headers
        next(csv_reader)
        next(tsv_reader)
        next(fixed_reader)

        # Combine rows from all three files
        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            combined_row = csv_row + tsv_row + fixed_row
            writer.writerow(combined_row)

# Function to transform data
def transform_data():
    input_file = f"{destination_path}/extracted_data.csv"
    output_file = f"{destination_path}/transformed_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Vehicle type'] = row['Vehicle type'].upper()
            writer.writerow(row)


# Default arguments for the DAG
default_args = {
    'owner': 'hosi04',
    'start_date': days_ago(0),
    'email': ['hosinguyenn@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)
untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)
extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)
extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)
# # Set the task dependencies
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
