# Use osgeo GDAL image.
#Ubuntu 20.04.4 LTS, Python 3.8.10, GDAL 3.4.2
FROM osgeo/gdal:ubuntu-small-3.4.2

ENV DIR=/usr/local/app
ENV TMP=/usr/local/tmp
ENV TILES=/usr/local/tiles
ENV DEBIAN_FRONTEND=noninteractive
ENV SECRETS_PATH /usr/secrets

# set timezone fo tzdata
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime

# Install dependencies
# PostGIS extension version based on https://computingforgeeks.com/how-to-install-postgis-on-ubuntu-linux/
RUN apt-get update -y && apt-get install -y \
    make \
    automake \
    g++ \
    gcc \
    libpq-dev \
    postgresql-12 \
    postgresql-server-dev-12 \
    postgresql-contrib-12 \
    postgresql-12-postgis-3 \
    python3-pip \
    wget \
    nano \
    htop \
    tmux \
    git \
    vim \
    zip \
    && apt-get clean all


# Create local app directory and copy repo
RUN mkdir -p ${DIR}
RUN mkdir -p ${TMP}
RUN mkdir -p ${TILES}
WORKDIR ${DIR}
COPY . .


# Set environment variables
ENV AWS_SHARED_CREDENTIALS_FILE $SECRETS_PATH/.aws/credentials
ENV AWS_CONFIG_FILE $SECRETS_PATH/.aws/config
# https://www.postgresql.org/docs/current/libpq-envars.html
ENV PGUSER postgres
ENV PGDATABASE=ubuntu


#######################################
# Activate postgres and enable connection to it
# Copies config file that allows user postgres to enter psql shell,
# as shown here: https://stackoverflow.com/a/26735105 (change peer to trust).
# Commented out the start/restart commands because even with running them, postgres isn't running when the container is created.
# So there's no point in starting posgres here if it's not active when the instance opens.
#######################################
RUN cp pg_hba.conf /etc/postgresql/12/main/
# RUN pg_ctlcluster 10 main start
# RUN service postgresql restart


# Install missing Python dependencies
RUN pip3 install -r requirements.txt

# Link gdal libraries
RUN cd /usr/include && ln -s ./ gdal

# # Somehow, this makes gdal_calc.py accessible from anywhere in the Docker
# #https://www.continualintegration.com/miscellaneous-articles/all/how-do-you-troubleshoot-usr-bin-env-python-no-such-file-or-directory/
# RUN ln -s /usr/bin/python3 /usr/bin/python

# Enable ec2 to interact with GitHub
RUN git config --global user.email dagibbs22@gmail.com

## Check out the branch that I'm currently using for model development
#RUN git checkout model_v_1.2.2
#
## Makes sure the latest version of the current branch is downloaded
#RUN git pull origin model_v_1.2.2

# Opens the Docker shell
ENTRYPOINT ["/bin/bash"]