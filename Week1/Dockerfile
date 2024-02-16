# set base image (python image) for your container
FROM python:3.9

# install dependencies
RUN pip install pandas pyarrow sqlalchemy psycopg2-binary

# Update package lists and install wget
RUN apt-get update && apt-get install -y wget
# set the working directory in the container to /app
# all subsequent commands will be run from this directory
WORKDIR /app

# copy pipeline.py from host machine into image, specifically into /app directory
COPY ingest_data.py ingest_data.py

# set entry point: when docker run, run pipeline.py
ENTRYPOINT [ "python", "ingest_data.py"]
