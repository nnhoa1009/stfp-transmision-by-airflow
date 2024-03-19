# stfp-transmision-by-airflow

## This repository is to solve the preliminary assessment problem [here](https://data-gcdn.basecdn.net/202403/sys8305/hiring/12/14/DHCHY5WJFE/e472079b3561e2ea177c6d4eeeacfcf3/DFFYK27DJMTMTYNKEQSYS624Z3FDQ6B9VQM68MZMDNGFJWK7GFKE5RNGB85HKFYNP8YH8XB6VXEQEKF49P73KD/f5/56/d4/25/9d/965ac629170ca1f2cebe8231e5c8e6b5/preliminary_assessment.pdf), please review the problem if you need it.

## Here is my entire source code and it includes:

### A `docker-compose` file (refer to the official Airflow docker manual page [here](https://airflow.apache.org/docs/apache-airflow/2.8.3/docker-compose.yaml) )

### A dag file with data flow assuming the following:
- I have a sensor that always waits for the `/home/demo/sftp/tx` directory on the `stfp_source` server to be present.
- When data is present in `sftp_source`, I read and perform a basic transformation that encrypts two sensitive information fields `accnbr` and `exec_phone_nbr`.
- After the data has been transformed, I write it to `sftp_dest` and proceed to delete that file in `sftp_source`.
- Next, I trigger this same DAG again to revive a new sensor.


## Instructions for installing and launching the project:

- First, make sure you have docker and docker compose installed on your computer. Go to the root directory and run `docker compose up -d`. Access Airflow UI at `localhost:8088` to make sure Airflow is working.

- Next, let's add 2 connections to the airflow database to connect to the 2 SFTP Servers we have. There are 2 ways, you can use Airflow UI to go to `Admin -> Connection -> Add a new record` or you can use the command `docker exec --it {YOUR_AIRFLOW_WEB_SERVER_CONTAIN_ID} /bin/bahs` to access the container and then use following command to add 2 connections:
``` 
    airflow connections add 'sftp_source' \
     --conn-json '{
         "conn_type": "sftp",
         "login": "demo",
         "password": "demo",
         "host": "sftp_source",
         "port": 22
     }'
```
``` 
    airflow connections add 'sftp_dest' \
     --conn-json '{
         "conn_type": "sftp",
         "login": "demo",
         "password": "demo",
         "host": "sftp_dest",
         "port": 22
     }'
```
The connections information is similar if you use Airflow UI
- Next go to Airflow UI, click to the DAG `sftp_tx_operations_workflow` and trigger it manually. Since I defined this DAG to run only when needed, I did not add interval scheduling.

**Note: You must use the `Trigger DAG` button in the upper right corner of the screen. If you just press `Unpause DAG`, the DAG will not automatically trigger.**

- To activate the sensor, copy the `tx.csv` file from the `sample_data` folder to the `source_data/tx` folder. The expected result is that the `tx.csv` file will be encrypted in 2 columns `exec_phone_nbr` and `acctnbr` then copied to the `dest_data/tx` folder and then deleted. To continue triggering the DAG a second time, you just need to create a simulated data from the sample csv file. Or copy the csv file from `sample` to `source_data/tf` again.


#### Thank you for reading. Wishing you a day full of energy and productivity.
