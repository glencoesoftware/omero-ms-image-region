FROM centos:centos7
MAINTAINER sysadmin@glencoesoftware.com
#LABEL org.openmicroscopy.release-date="unknown"
#LABEL org.openmicroscopy.commit="unknown"

RUN yum -y install epel-release && yum -y install ansible sudo unzip which


RUN curl -L -o /usr/local/bin/dumb-init \
    https://github.com/Yelp/dumb-init/releases/download/v1.2.0/dumb-init_1.2.0_amd64 && \
    chmod +x /usr/local/bin/dumb-init
ADD entrypoint.sh /usr/local/bin/
ADD 99-run.sh /startup/

RUN mkdir /opt/setup
WORKDIR /opt/setup

# this is what passes for artifact deployment in docker land
RUN mkdir -p /opt/omero/ms/
ADD omero-ms-image-region-0.4.3.tar.gz /opt/omero/ms/
RUN cd /opt/omero/ms && ln -sv omero-ms-image-region-0.4.3 image-region

ADD playbook.yml *.j2 /opt/setup/
RUN ansible-playbook playbook.yml

USER oms-imageregion

EXPOSE 8080
#VOLUME ["/opt/omero/ms/image-region/logs"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
