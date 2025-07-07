# Base image resmi Flink (1.17) dengan Scala 2.12
FROM flink:1.17-scala_2.12

# Install Python dan pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    python -m pip install --upgrade pip

# Install PyFlink (versi cocok dengan Flink 1.17 = PyFlink 1.17)
RUN pip install apache-flink==1.17.0

# Tambahkan JAR Kafka dan JSON format (pastikan file ada di folder yang sama dengan Dockerfile)
COPY ./lib /opt/flink/lib/



# Set working directory
WORKDIR /opt/flink

