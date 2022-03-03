FROM apache/airflow:2.2.4
USER root

RUN apt-get update && \
    apt install -y gcc python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev g++ && \
    apt-get install -y iputils-ping

RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list                                                   
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk

RUN apt-get install unzip -y && \
    apt-get autoremove -y

USER airflow
COPY requirements.txt /
workdir /
RUN cat /etc/*-release
RUN python3 --version
RUN pip3 install --upgrade pip  
RUN pip3 install --no-cache -r requirements.txt