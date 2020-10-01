FROM "openmicroscopy/omero-server:5.4.10"
MAINTAINER sysadmin@glencoesoftware.com
#LABEL org.openmicroscopy.release-date="unknown"
#LABEL org.openmicroscopy.commit="unknown"

# adding a tar.gz will auto uncompress it in the target location
ADD omero-ms-image-region-*.tar.gz /opt/omero/ms/

USER root
# ms start script requires which to find java
RUN yum -y install which
# remove omero-server startup scripts
RUN find /startup/ -type f -delete
# add our own startup script
ADD 99-run.sh /startup/

# create symlink to versioned directory
RUN cd /opt/omero/ms && ln -sv $( basename $( find /opt/omero/ms/ -type d -name omero-ms-image-region-\* -maxdepth 1 -print ) ) image-region

# create ms directory and chown to proper user
RUN mkdir -p /opt/omero/ms/ && chown -R omero-server:omero-server /opt/omero/ms

USER omero-server

EXPOSE 8080
#VOLUME ["/opt/omero/ms/image-region/logs"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
