FROM "openmicroscopy/omero-server"
MAINTAINER sysadmin@glencoesoftware.com
#LABEL org.openmicroscopy.release-date="unknown"
#LABEL org.openmicroscopy.commit="unknown"


USER root
RUN yum -y install which
RUN find /startup/ -type f -delete
ADD 99-run.sh /startup/
RUN mkdir -p /opt/omero/ms/ && chown omero-server:omero-server /opt/omero/ms

USER omero-server
ADD omero-ms-image-region-*.tar.gz /opt/omero/ms/
RUN find /opt/omero/ms/ -type d -name omero-ms-image-region-\* -maxdepth 1 -print 
RUN cd /opt/omero/ms && ln -sv $( basename $( find /opt/omero/ms/ -type d -name omero-ms-image-region-\* -maxdepth 1 -print ) ) image-region

EXPOSE 8080
#VOLUME ["/opt/omero/ms/image-region/logs"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
