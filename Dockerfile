# Development Dockerfile for Microservices
# ----------------------------------------
# This dockerfile can be used to build
# a distribution which can then be run
# within a number of different Docker images.

# By default, building this dockerfile will use
# the IMAGE argument below for the runtime image:

ARG IMAGE=adoptopenjdk/openjdk11:jre-11.0.7_10-alpine

# To install the built distribution into other runtimes
# pass a build argument, e.g.:
#
#   docker build --build-arg IMAGE=openjdk:9 ...
#

# Similarly, the GRADLE_IMAGE argument can be overwritten
# but this is generally not needed.
ARG GRADLE_IMAGE=gradle:jdk

#
# Build phase: Use the gradle image for building.
#
FROM ${GRADLE_IMAGE} as gradle
RUN mkdir -p ms

COPY build.gradle ms/
COPY src ms/src
COPY gradle ms/gradle
COPY gradlew ms/gradlew
WORKDIR ms
RUN /bin/bash ./gradlew installDist


#
# Install phase: Copy the built distribution into a
# clean container to minimize size.
#
FROM ${IMAGE}
COPY --from=gradle /home/gradle/ms/build/install/ms /opt/ms

EXPOSE 8080
ENV JAVA_OPTS "-Xmx1G"

WORKDIR /opt/ms


ARG OMERO_SERVER=omero
ARG REDIS_SERVER=redis
RUN sed -i "s/127.0.0.1:6379/$REDIS_SERVER:6379/" conf/config.yaml
RUN sed -i "s/localhost/$OMERO_SERVER/" conf/config.yaml

ENTRYPOINT ["sh", "bin/ms"]
