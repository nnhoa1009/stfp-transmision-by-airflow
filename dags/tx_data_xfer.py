
from datetime import datetime, timedelta
from configparser import ConfigParser
import os

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from helpers.tx_processing import SFTPTransferHelper

with DAG("sftp_tx_operations_workflow",
         schedule_interval=None,
         start_date=datetime(2024, 3, 1)) as dag:
    
    FIRST_DATE_OF_2000='20000101000000'
    helper = SFTPTransferHelper()
    config_object = ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'sftp_xfer.cfg')
    config_object.read(config_path)
    init_conf = config_object['init']
    latest_processed_time_str = init_conf["latest_tx_file_processed_time"] or FIRST_DATE_OF_2000 #init latest processed time in the first time.
    latest_processed_time = datetime.strptime(latest_processed_time_str, '%Y%m%d%H%M%S')
    # latest_processed_time = latest_processed_time + timedelta(seconds=1)
    
    base_path = init_conf["base_path"]
    tx_path = f"{base_path}/tx"
    
    wait_for_tx_data = SFTPSensor(task_id="wait_for_tx_data",
                                     sftp_conn_id='sftp_source',
                                     path=tx_path,
                                     file_pattern='*.csv',
                                     newer_than=latest_processed_time,
                                     mode='reschedule',
                                     poke_interval=10)
    
    # debug = PythonOperator(task_id="debug",
    #                               templates_dict={
    #                                   "tx_path": tx_path,
    #                                   "latest_processed_time_str": str(latest_processed_time_str)
    #                               },
    #                               python_callable=helper.get_latest_file1)

    latest_file, latest_processed_time = helper.get_latest_file(tx_path, latest_processed_time_str)
    config_object["init"]["latest_tx_file_processed_time"] = latest_processed_time
    with open(config_path, 'w') as conf:
        config_object.write(conf)
        
    self_trigger = TriggerDagRunOperator(task_id='self_trigger', trigger_dag_id=dag.dag_id)
        
    remote_filepath = f"{tx_path}/{latest_file}"
    tx_local_file_path = "/tmp/{{ run_id }}/tx/" + latest_file
    
    get_tx_file = SFTPOperator(
        task_id="get_tx_file",
        ssh_conn_id="sftp_source",
        remote_filepath=remote_filepath,
        local_filepath=tx_local_file_path,
        operation="get",
        create_intermediate_dirs=True
    )

    local_output_file_path = "/tmp/{{ run_id }}/output_" + latest_file
    transform_tx_data = PythonOperator(task_id="transform_tx_data",
                                  templates_dict={
                                      "input_file": tx_local_file_path,
                                      "output_file": local_output_file_path,
                                      "sensitive_cols": ["exec_phone_nbr", "acctnbr"]
                                  },
                                  python_callable=helper.process_file)

    upload_tx_file = SFTPOperator(
        task_id="upload_tx_file",
        ssh_conn_id="sftp_dest",
        remote_filepath=remote_filepath,
        local_filepath=local_output_file_path,
        create_intermediate_dirs=True,
        operation="put"
    )
    
    clean_tx_data = PythonOperator(task_id="clean_tx_data",
                                  templates_dict={
                                      "file_path": remote_filepath
                                  },
                                  python_callable=helper.clean_processed_file)

    wait_for_tx_data >> get_tx_file >> transform_tx_data >> upload_tx_file >> clean_tx_data >> self_trigger