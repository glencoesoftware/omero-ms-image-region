[![AppVeyor status](https://ci.appveyor.com/api/projects/status/github/omero-ms-image-region)](https://ci.appveyor.com/project/gs-jenkins/omero-ms-image-region)

OMERO Image Region Microservice
===============================

OMERO image region and shape mask Vert.x asynchronous microservice server
endpoint for OMERO.web.

Requirements
============

* OMERO 5.4.x+
* OMERO.web 5.4.x+
* Redis backed sessions
* Java 8+

Workflow
========

The microservice server endpoint for OMERO.web relies on the following
workflow::

1. Setup of OMERO.web to use Redis backed sessions

1. Configuring the microservice endpoint

1. Ensure the microservice can communicate with your PostgreSQL instance

1. Ensure the microservice has read-write access to your OMERO server binary
repository.

1. Running the microservice endpoint for OMERO.web

1. Redirecting your OMERO.web installation to use the microservice endpoint

Build Artifacts
===============

The latest artifacts, built by AppVeyor, can be found here::

* https://ci.appveyor.com/project/gs-jenkins/omero-ms-image-region/build/artifacts

Configuring and Running the Server
==================================

The image region microservice server endpoint piggybacks on the OMERO.web Django
session, OMERO database and binary repository.  As such it is essential that as
a prerequisite to running the server that your OMERO.web instance be configured
to use Redis backed sessions, the microservice be able to communicate with your
PostgreSQL instance, and that the binary repository be read-write accessible.
Filesystem backed sessions **are not** supported.

1. Configure the application by editing `conf/config.yaml`

1. Start the server::

        omero-ms-image-region

Configuring Logging
-------------------

Logging is provided using the logback library.  You can configure logging by
copying the included `logback.xml.example`, editing as required, and then
specifying the configuration when starting the microservice server::

    cp logback.xml.example logback.xml
    ...
    JAVA_OPTS="-Dlogback.configurationFile=/path/to/logback.xml" \
        omero-ms-image-region ...

Debugging the logback configuration can be done by providing the additional
`-Dlogback.debug=true` property.

Using systemd
-------------

If you are using `systemd` you can place an appropriately modified version of
the included `omero-ms-image-region.service` into your `/etc/systemd/system`
directory and then execute::

    systemctl daemon-reload
    systemctl start omero-ms-image-region.service

Running `systemctl status omero-ms-image-region.service` will then produce
output similar to the following::

    # systemctl status omero-ms-image-region.service
    ● omero-ms-image-region.service - OMERO image region microservice server
       Loaded: loaded (/etc/systemd/system/omero-ms-image-region.service; disabled; vendor preset: disabled)
       Active: active (running) since Thu 2017-06-01 14:40:53 UTC; 8min ago
     Main PID: 9096 (java)
       CGroup: /system.slice/omero-ms-image-region.service
               └─9096 java -Dlogback.configurationFile=/opt/omero/omero-ms-image-region-0.1.0-SNAPSHOT/logback.xml -classpath /opt/omero/omero-ms-image-region-0.1.0-SNAPSHOT/lib/omero-ms-image-region-0.1.0-SNAPSHOT.jar:/opt/omero/omero-...

    Jun 01 14:40:53 demo.glencoesoftware.com systemd[1]: Started OMERO image region microservice server.
    Jun 01 14:40:53 demo.glencoesoftware.com systemd[1]: Starting OMERO image region microservice server...
    Jun 01 14:40:54 demo.glencoesoftware.com omero-ms-image-region[9096]: Jun 01, 2017 2:40:54 PM io.vertx.core.spi.resolver.ResolverProvider
    Jun 01 14:40:54 demo.glencoesoftware.com omero-ms-image-region[9096]: INFO: Using the default address resolver as the dns resolver could not be loaded
    Jun 01 14:40:55 demo.glencoesoftware.com omero-ms-image-region[9096]: Jun 01, 2017 2:40:55 PM io.vertx.core.Starter
    Jun 01 14:40:55 demo.glencoesoftware.com omero-ms-image-region[9096]: INFO: Succeeded in deploying verticle

Regenerating memo files
-----------------------

Bio-Formats memo files often need to be regenerated between Bio-Formats
upgrades.  The image region microservice ships with a command line tool,
`memoregenerator`, to perform this regeneration out of band using a secondary
Bio-Formats cache directory.

1. Configure the application by editing `conf/config.yaml`

1. Run the input CSV generator against your database::

        psql -f memo_regenerator.sql omero > input.csv

1. Split the input CSV into as many jobs as required::

        split -l <images_per_job> --additional-suffix=.csv input.csv input

1. Run memo file regeneration to a secondary directory, in parallel if desired::

        memoregenerator --cache-dir /other/cache/dir input...csv

Redirecting OMERO.web to the Server
===================================

What follows is a snippet which can be placed in your nginx configuration,
**before** your default OMERO.web location handler, to redirect both
*webgateway* image region rendering currently used by OMERO.web to the
image region microservice server endpoint::

    upstream image_region_backend {
        server 127.0.0.1:8080 fail_timeout=0 max_fails=0;
    }

    ...

    location /webgateway/render_image_region/ {
        proxy_pass http://image_region_backend;
    }

    location /webgateway/render_image/ {
        proxy_pass http://image_region_backend;
    }

    location /webclient/render_image_region/ {
        proxy_pass http://image_region_backend;
    }

    location /webclient/render_image/ {
        proxy_pass http://image_region_backend;
    }

    location /webgateway/render_shape_mask/ {
        proxy_pass http://image_region_backend;
    }

Development Installation
========================

1. Clone the repository::

        git clone git@github.com:glencoesoftware/omero-ms-image-region.git

1. Run the Gradle build and utilize the artifacts as required::

        ./gradlew installDist
        cd build/install
        ...

1. Log in to the OMERO.web instance you would like to develop against with
your web browser and with the developer tools find the `sessionid` cookie
value. This is the OMERO.web session key.

1. Run single or multiple image region tests using `curl`::

        curl -H 'Cookie: sessionid=<omero_web_session_key>' \
            http://localhost:8080/webgateway/render_image/<image_id>/<z>/<t>/

        curl -H 'Cookie: sessionid=<omero_web_session_key>' \
            http://localhost:8080/webgateway/render_image_region/<image_id>/<z>/<t>/?tile=0,0,0,1024,1024

Eclipse Configuration
=====================

1. Run the Gradle Eclipse task::

        ./gradlew eclipse

1. Configure your environment::

        mkdir conf
        cp src/dist/conf/config.yaml conf/
        # Edit as appropriate

1. Add a new Run Configuration with a main class of `io.vertx.core.Launcher`::

        run "com.glencoesoftware.omero.ms.image.region.ImageRegionMicroserviceVerticle"

Running Tests
=============

Using Gradle run the unit tests:

    ./gradlew test

Reference
=========

* https://github.com/glencoesoftware/omero-ms-core
* https://lettuce.io/
* http://vertx.io/
