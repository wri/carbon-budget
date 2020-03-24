# Use osgeo GDAL image. It builds of Ubuntu 18.04 and uses GDAL 3.0.4
FROM osgeo/gdal:ubuntu-small-3.0.4

ENV DIR=/usr/local/app
ENV DEBIAN_FRONTEND=noninteractive
ENV SECRETS_PATH /usr/secrets

# set timezone fo tzdata
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime

# Install missing dependencies
RUN apt-get update -y && apt-get install -y \
    make \
    automake \
    g++ \
    gcc \
    libpq-dev \
    postgresql-10 \
    postgresql-server-dev-10 \
    postgresql-contrib-10 \
    postgresql-10-postgis-2.4 \
    python3-pip \
    wget \
    nano \
    htop \
    tmux \
    && apt-get clean all


# Create local app directory and copy repo
RUN mkdir -p ${DIR}
WORKDIR ${DIR}
COPY . .

# set environment variables
ENV AWS_SHARED_CREDENTIALS_FILE $SECRETS_PATH/.aws/credentials
ENV AWS_CONFIG_FILE $SECRETS_PATH/.aws/config

# Install missing python dependencies
RUN pip3 install -r requirements.txt

# Link gdal libraries
RUN cd /usr/include && ln -s ./ gdal

## Compile C++ scripts
#RUN g++ emissions/cpp_util/calc_gross_emissions_generic.cpp -o emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal && \
#    g++ emissions/cpp_util/calc_gross_emissions_soil_only.cpp -o emissions/cpp_util/calc_gross_emissions_soil_only.exe -lgdal && \
#    g++ emissions/cpp_util/calc_gross_emissions_no_shifting_ag.cpp -o emissions/cpp_util/calc_gross_emissions_no_shifting_ag.exe -lgdal && \
#    g++ emissions/cpp_util/calc_gross_emissions_convert_to_grassland.cpp -o emissions/cpp_util/calc_gross_emissions_convert_to_grassland.exe -lgdal

# Opens the Docker shell
ENTRYPOINT ["/bin/bash"]