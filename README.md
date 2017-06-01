[![AppVeyor status](https://ci.appveyor.com/api/projects/status/github/omero-ms-image-region)](https://ci.appveyor.com/project/gs-jenkins/omero-ms-image-region)

OMERO Tiling Microservice
=========================

OMERO tiling  Vert.x asynchronous microservice server endpoint for OMERO.web.

Requirements
============

* OMERO 5.2.x+
* OMERO.web 5.2.x+
* Redis backed sessions
* Java 8+

Workflow
========

The microservice server endpoint for OMERO.web relies on the following
workflow::

1. Setup of OMERO.web to use database or Redis backed sessions

1. Running the microservice endpoint for OMERO.web

1. Redirecting your OMERO.web installation to use the microservice endpoint

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

1. Run single or multiple thumbnail tests using `curl`::

        curl -H 'Cookie: sessionid=<omero_web_session_key>' \
            http://localhost:8080/render_image_region/<image_id>/<z>/<t>/?tile=0,0,0,1024,1024

Eclipse Configuration
=====================

1. Run the Gradle Eclipse task::

        ./gradlew eclipse

1. Configure your environment::

        cp conf.json.example conf.json

1. Add a new Run Configuration with a main class of `io.vertx.core.Starter`::

        run "com.glencoesoftware.omero.ms.thumbnail.ThumbnailMicroserviceVerticle" \
            -conf "conf.json"

Build Artifacts
===============

The latest artifacts, built by AppVeyor, can be found here::

* https://ci.appveyor.com/project/gs-jenkins/omero-ms-image-region/build/artifacts

Configuring and Running the Server
==================================

The thumbnail microservice server endpoint piggybacks on the OMERO.web Django
session.  As such it is essential that as a prerequisite to running the
server that your OMERO.web instance be configured to use Redis backed sessions.
Filesystem backed sessions **are not** supported.

1. Configure the application::

        cp conf.json.example path/to/conf.json

1. Start the server::

        omero-ms-image-region -conf path/to/conf.json

Redirecting OMERO.web to the Server
===================================

What follows is a snippet which can be placed in your nginx configuration,
**before** your default OMERO.web location handler, to redirect both
*webclient* and *webgateway* thumbnail rendering currently used by OMERO.web
to the thumbnail microservice server endpoint::

    upstream image-region-backend {
        server 127.0.0.1:8080 fail_timeout=0 max_fails=0;
    }

    ...

    location /webgateway/render_image_region/ {
        proxy_pass http://image_region_backend;
    }

    location /webgateway/render_image_region/ {
        proxy_pass http://image-region-backend;
    }

Running Tests
=============

Using Gradle run the unit tests:

    ./gradlew test

Reference
=========

* https://github.com/glencoesoftware/omero-ms-core
* https://lettuce.io/
* http://vertx.io/
