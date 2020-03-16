# Use osgeo GDAL image. It builds of Ubuntu 18.04 and uses GDAL 3.0.4
FROM osgeo/gdal:ubuntu-small-3.0.4

ENV DIR=/usr/local/app
ENV DEBIAN_FRONTEND=noninteractive

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
    && apt-get clean all


# Create local app directory and copy repo
RUN mkdir -p ${DIR}
WORKDIR ${DIR}
COPY . .

# Install missing python dependencies
RUN pip3 install -r requirements.txt

# Link gdal libraries
RUN cd /usr/include && ln -s ./ gdal

# Compile C++ scripts
RUN g++ emissions/cpp_util/calc_gross_emissions_generic.cpp -o emissions/cpp_util/calc_gross_emissions_generic -lgdal && \
    g++ emissions/cpp_util/calc_gross_emissions_soil_only.cpp -o emissions/cpp_util/calc_gross_emissions_soil_only -lgdal && \
    g++ emissions/cpp_util/calc_gross_emissions_no_shifting_ag.cpp -o emissions/cpp_util/calc_gross_emissions_no_shifting_ag -lgdal && \
    g++ emissions/cpp_util/calc_gross_emissions_convert_to_grassland.cpp -o emissions/cpp_util/calc_gross_emissions_convert_to_grassland -lgdal


# Set current work directory to /tmp. This is important when running as AWS Batch job
# When using the ephemeral-storage launch template /tmp will be the mounting point for the external storage
# In AWS batch we will then mount host's /tmp directory as docker volume /tmp
WORKDIR /tmp

ENTRYPOINT ["python run_full_model.py"]
