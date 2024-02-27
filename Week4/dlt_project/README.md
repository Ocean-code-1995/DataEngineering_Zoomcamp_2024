dlt docs set up:
    - https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem

virtual env:
╰─ conda create --prefix ./.env/week4_conda_env
╰─ conda activate .env/week4_conda_env


- init dlt project:
  - creates toml files for gcp authentication
    - command: dlt init taxi_data_pipeline filesystem
