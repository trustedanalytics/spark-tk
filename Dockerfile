# Build from the Java 8 maven image (include JDK 8 and Maven)
FROM maven:3.3-jdk-8

# Expose Port 8080
EXPOSE 8080

# ADD SETTINGS.XML FOR MAVEN PROXY SETTINGS
# ADD ./settings.xml /root/.m2/settings.xml

# CREATE SOURCE DIRECTORY
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# INSTALL REQUIRED PACKAGES
RUN apt-get -q clean && \
    apt-get -q update && \
    apt-get -q install -y \
      build-essential \
      bzip2 \
      gcc \
      gfortran \
      libxml2-dev \ 
      libxslt1-dev \
      python-qt4 \
      python-dev \
      wget \
      zip \
      unzip; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# INSTALL PIP
RUN wget -nv https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py

# DOWNLOAD SPARK BINARY AND SET SPARK_HOME
RUN wget -nv http://archive.apache.org/dist/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
RUN tar xzf spark-1.6.0-bin-hadoop2.6.tgz
RUN mv spark-1.6.0-bin-hadoop2.6 spark
RUN rm spark-1.6.0-bin-hadoop2.6.tgz

ENV SPARK_HOME /usr/src/app/spark

# INSTALL GRAPHFRAMES DEPENDENCY
RUN wget -nv --no-check-certificate http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.1.0-spark1.6/graphframes-0.1.0-spark1.6.jar -O graphframes.zip
RUN unzip -q graphframes.zip
RUN ln -s `pwd`/graphframes /usr/lib/python2.7/dist-packages/graphframes
RUN rm -rf graphframes.zip

# CREATE SPARK-TK DIRECTORY AND COPY SOURCE FILES
RUN mkdir -p /usr/src/app/spark-tk
COPY ./ spark-tk/

# CHDIR TO SPARK-TK DIRECTORY
WORKDIR /usr/src/app/spark-tk

# INSTALL PYTHON DEPENDENCIES
RUN pip install -r python/requirements.txt

# BUILD SPARK-TK [ RUNS UNIT TESTS AND INTEGRATION TESTS ]
RUN mvn clean install -q

# ADD SPARK-TK TO PYTHONPATH
ENV PYTHONPATH /usr/src/app/spark-tk/python

# LAUNCH PYTHON [ docker run -it <image_name> ]
CMD ["python"]
