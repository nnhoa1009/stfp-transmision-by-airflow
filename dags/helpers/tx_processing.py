import hashlib
import pandas as pd
from airflow.providers.sftp.hooks.sftp import SFTPHook

class SFTPTransferHelper():
    def process_file(self, **kwargs):
        templates_dict = kwargs.get("templates_dict")
        input_file = templates_dict.get("input_file")
        sensitive_cols = templates_dict.get("sensitive_cols")
        output_file = templates_dict.get("output_file")
        
        data = pd.read_csv(input_file)
        if len(sensitive_cols) > 0:
            for col in sensitive_cols:
                data[col] = data[col].astype(str)
                data[col] = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in data[col]]
        
        data.to_csv(output_file, index=False)
            
    def get_latest_file(self, tx_path: str, latest_processed_time):
        sftp_hook = SFTPHook(ssh_conn_id='sftp_source')
        lst_files = sftp_hook.describe_directory(tx_path)
        last_modified_timestamp = int(latest_processed_time)
        result = ''
        for f in lst_files:
            if int(lst_files[f]['modify']) >= last_modified_timestamp:
                last_modified_timestamp = int(lst_files[f]['modify'])
                result = f
        return result, str(last_modified_timestamp)
    
    def clean_processed_file(self, **kwargs):
        templates_dict = kwargs.get("templates_dict")
        file_path = templates_dict.get("file_path")
        sftp_hook = SFTPHook(ssh_conn_id='sftp_source')
        sftp_hook.delete_file(file_path)
        return True
    
    # def get_latest_file1(self, **kwargs):
    #     templates_dict = kwargs.get("templates_dict")
    #     tx_path = templates_dict.get("tx_path")
    #     latest_processed_time = templates_dict.get("latest_processed_time_str")
    #     sftp_hook = SFTPHook(ssh_conn_id='sftp_source')
    #     lst_files = sftp_hook.describe_directory(tx_path)
    #     print(lst_files)
    #     last_modified_timestamp = int(latest_processed_time)
    #     result = ''
    #     for f in lst_files:
    #         if int(lst_files[f]['modify']) >= last_modified_timestamp:
    #             last_modified_timestamp = int(lst_files[f]['modify'])
    #             result = f
    #     print(result)
    #     return result, str(last_modified_timestamp)
        