#FROM registry.fedoraproject.org/fedora:40
FROM fedora:latest

RUN dnf update -y && dnf install -y python3 python3-pip gcc make curl && dnf clean all

RUN useradd --create-home cdeuser
USER cdeuser
WORKDIR /home/cdeuser

RUN mkdir /home/cdeuser/.cde
RUN mkdir /home/cdeuser/.cdp
RUN chmod 777 /home/cdeuser/.cde
RUN chmod 777 /home/cdeuser/.cdp
RUN chmod 777 /home/cdeuser/

RUN pip3 install --no-cache-dir jupyter \
  jupyterlab
RUN pip3 install numpy \
  pandas \
  matplotlib \
  scikit-learn

ADD img /home/cdeuser/img
ADD de-pipeline-manufacturing /home/cdeuser/de-pipeline-manufacturing
ADD de-pipeline-bank /home/cdeuser/de-pipeline-bank
ADD setup /home/cdeuser/setup
ADD config.yaml /home/cdeuser/.cde/config.yaml
ADD credentials /home/cdeuser/.cdp/credentials
ADD cde /usr/bin/cde
COPY cdeconnect.tar.gz /home/cdeuser/cdeconnect.tar.gz
COPY pyspark-3.5.1.tar.gz /home/cdeuser/pyspark-3.5.1.tar.gz
ADD Iceberg_TimeTravel_PySpark_banking.ipynb /home/cdeuser/Iceberg_TimeTravel_PySpark_banking.ipynb
ADD Iceberg_TimeTravel_PySpark_manufacturing.ipynb /home/cdeuser/Iceberg_TimeTravel_PySpark_manufacturing.ipynb
ADD pyspark-app.py /home/cdeuser/pyspark-app.py

EXPOSE 8888

CMD ["python3", "-m", "jupyterlab", "--ip='0.0.0.0'", "--port=8888", "--allow-root"]
