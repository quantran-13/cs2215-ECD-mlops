# cs2215-ECD-mlops
## Overview & requirements: 
* [Link to requirements](https://drive.google.com/file/d/1ZtzM0-IVmwG1uhKN58rpUNuyUZrL1gQQ/view?usp=share_link)
* [Link to data](https://drive.google.com/file/d/1csLsV2JN2U2BNntrz3QiJiTjtf7kMTgA/view?usp=share_link)

## Project structure
```
./ 
    ./clearml-server    # ClearML server  
        README.md       # Follow the guide here  
        ...

    ./app-api           # API for demo app & launch task into clearML server  
        README.md       # Follow the guide here  
        ...

    ./app-frontend      # Streamlit demo app
        README.md       # Follow the guide here  
        ...
    ./src               # Training, ETL code 
        ...
```

## Starting the project:  
The project compose of 5 components, you must start these in order: 
1. Start ClearML Server:   
    * Use docker-compose: `cd ./clearml-server/docker` & `docker-compose up`

2. [ClearML Agent](https://clear.ml/docs/latest/docs/clearml_agent/), follow the documentation link to config 

3. Clone [Airflow + DAG](https://github.com/quantran-13/cs2215-ECD-airflow.git) into different directory. 
    * 3.2. `cd` into that repo, starting Airflow via `docker-compose up` 
    * 3.3. Adding DAG via `python ./dags/demo` 

4. `app-api`, `cd` into `app-api` and follow the `readme` 
5. `app-frontend`, `cd` into `app-api` and follow the `readme` 


## Note: 
- `app-api` store code to launch training task, hyperparameter optimization, ETL.
- `app-api` contains hard-coded param to launch task into clearML server, you might want to change it on you needs. 