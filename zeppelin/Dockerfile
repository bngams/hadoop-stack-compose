FROM apache/zeppelin:0.12.0

USER root

# Install Python and required packages
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3.8-dev && \
    rm -rf /var/lib/apt/lists/*

# Install pandas and other common data science libraries for Python 3.8
RUN python3.8 -m pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib \
    pyarrow \
    scikit-learn \
    seaborn \
    plotly \
    openpyxl \
    xlrd

# Set Python environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Create necessary directories
RUN mkdir -p /opt/hadoop/conf

WORKDIR /opt/zeppelin
