# Use osgeo GDAL image. It builds off Ubuntu 18.04 and uses GDAL 3.0.4
FROM osgeo/gdal:ubuntu-small-3.0.4
#FROM osgeo/gdal:ubuntu-full-3.0.4   # Use this if downloading hdf files for burn year analysis

ENV DIR=/usr/local/app
ENV TMP=/usr/local/tmp
ENV TILES=/usr/local/tiles
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
    git \
    vim \
    && apt-get clean all


# Create local app directory and copy repo
RUN mkdir -p ${DIR}
RUN mkdir -p ${TMP}
RUN mkdir -p ${TILES}
WORKDIR ${DIR}
COPY . .

# set environment variables
ENV AWS_SHARED_CREDENTIALS_FILE $SECRETS_PATH/.aws/credentials
ENV AWS_CONFIG_FILE $SECRETS_PATH/.aws/config

# Install missing python dependencies
RUN pip3 install -r requirements.txt

# Link gdal libraries
RUN cd /usr/include && ln -s ./ gdal

# Somehow, this makes gdal_calc.py accessible from anywhere in the Docker
#https://www.continualintegration.com/miscellaneous-articles/all/how-do-you-troubleshoot-usr-bin-env-python-no-such-file-or-directory/
RUN ln -s /usr/bin/python3 /usr/bin/python

# Check out the branch that I'm currently using for model development
RUN git checkout model_v_1.2.0

# Makes sure the latest version of the current branch is downloaded
RUN git pull origin model_v_1.2.0

# Compile C++ scripts
RUN g++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_generic.exe -lgdal && \
    g++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_soil_only.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_soil_only.exe -lgdal && \
    g++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_no_shifting_ag.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_no_shifting_ag.exe -lgdal && \
    g++ /usr/local/app/emissions/cpp_util/calc_gross_emissions_convert_to_grassland.cpp -o /usr/local/app/emissions/cpp_util/calc_gross_emissions_convert_to_grassland.exe -lgdal

# Opens the Docker shell
ENTRYPOINT ["/bin/bash"]