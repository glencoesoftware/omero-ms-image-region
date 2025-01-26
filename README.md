OMERO Image Region Microservice
===============================

OMERO image region and shape mask Vert.x asynchronous microservice server
endpoint for OMERO.web.

Requirements
============

* OMERO 5.6.x+
* OMERO.web 5.6.x+
* Redis backed sessions
* Java 11+

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

Redirecting OMERO.web to the Server
-----------------------------------

What follows is a snippet which can be placed in your nginx configuration,
**before** your default OMERO.web location handler, to redirect both
*webgateway* image region rendering currently used by OMERO.web to the
image region microservice server endpoint::

    upstream image_region_backend {
        server 127.0.0.1:8081 fail_timeout=0 max_fails=0;
    }

    ...


    location ~ ^/(webclient|webgateway)/(render_(thumbnail_ngff|image|image_region|image_region_rdef|image_rdef|shape_mask)|get_thumbnails_ngff|histogram_json)/ {
      proxy_pass http://image_region_backend;
    }

    location /omero_ms_image_region/ {
        gzip on;
        gzip_min_length 0;
        gzip_proxied any;
        gzip_types application/octet-stream;
        proxy_pass http://image_region_backend;
    }

Regenerating memo files
=======================

Bio-Formats memo files often need to be regenerated between OMERO upgrades.
The image region microservice ships with 3 command-line utilities:

-   `memoregenerator`: a script to perform this regeneration out of band using
    a secondary Bio-Formats cache directory
-   [regen-memo-files.sh](src/dist/regen-memo-files.sh): a wrapper around
    `memoregenerator`  to schedule the regeneration of all memo files using
    `parallel`
-   [memofile-regen-status.sh](src/dist/memofile-regen-status.sh): a script to
    check the status of memo file regeneration

Requirements & caveats
----------------------

In addition the micro-service requirements, the `parallel`, `time` and `bc`
packages are required to run the memo file regeneration. It is also strongly
recommended to start the memo file regeneration in a `screen` or `tmux`
session.

The script is designed as follows:

1.  run a SQL query to create a list of entries to regenerate ordered by their
    import initialization time
2.  split this list into input sets of 500 (by default) using round-robin
    distribution
3.  start a job processing all the sets created a set (2) in parallel

By default, the number of concurrent jobs is set to the number of available
CPUs. Each Java memo regeneration process is memory constrained to 2G. This
means you can run your system out of memory by setting the number too high
and/or have the memo regeneration process terminated by the OOM killer. For
instance, in a system with 8 CPUs/16G RAM an no other processes running, the
memo regeneration with 8 jobs can run the system out of memory.

Before starting the memo file regeneration processes, check the available
memory (`free` or `top`), and adjust the number of concurrent jobs using
the `--jobs` option.

Usage
-----

The help command will display all the available options:

    ./regen-memo-files.sh --help

The `--cache-options` option specifies which folder to use for regenerating the
new memo file and the `--jobs` option defines how many concurrent processes
will run:

    ./regen-memo-files.sh --cache-options /OMERO/BioFormatsCache.new --jobs 4

    CSV (image-list-20200313.14374.csv) not found, generating from database...
    running sql to generate images file
    85545 image-list-20200313.14374.csv images to process using 2 threads...starting

    Computers / CPU cores / Max jobs to run
    1:local / 8 / 4

    Computer:jobs running/jobs completed/%of started jobs/Average seconds to complete
    ETA: 0s Left: 8 AVG: 0.00s  local:2/0/100%/0.0s rslt.20200313.14374/1/rslt.20200313.14374\_inputaa.csv/stdout
    ETA: 573s Left: 7 AVG: 89.00s  local:2/1/100%/201.0s rslt.20200313.14374/1/rslt.20200313.14374\_inputab.csv/stdout
    ETA: 602s Left: 6 AVG: 102.00s  local:2/2/100%/158.0s rslt.20200313.14374/1/rslt.20200313.14374\_inputac.csv/stdout
    ETA: 474s Left: 5 AVG: 95.33s  local:2/3/100%/132.7s rslt.20200313.14374/1/rslt.20200313.14374\_inputad.csv/stdout
    ETA: 407s Left: 4 AVG: 102.25s  local:2/4/100%/130.2s rslt.20200313.14374/1/rslt.20200313.14374\_inputae.csv/stdout
    ETA: 303s Left: 3 AVG: 101.20s  local:2/5/100%/123.6s rslt.20200313.14374/1/rslt.20200313.14374\_inputaf.csv/stdout
    ETA: 208s Left: 2 AVG: 104.17s  local:2/6/100%/122.8s rslt.20200313.14374/1/rslt.20200313.14374\_inputag.csv/stdout
    ETA: 100s Left: 1 AVG: 101.00s  local:1/7/100%/117.0s rslt.20200313.14374/1/rslt.20200313.14374\_inputah.csv/stdout
    ETA: 0s Left: 0 AVG: 88.38s  local:0/8/100%/102.4s

    real    13m38.679s
    user    26m7.985s
    sys     1m18.327s

`--batch-size` allows to modify the number of entries in each input file. This
can be useful in systems with very large number of entries to regenerate:
The CSV file is split into input files with suffixes of length 3, meaning it can only generate up to 999 input files with each file defaulting to 500 lines each.  This means that an OMERO system with more than 499,500 images will need a larger batch size than 500.
    ./regen-memo-files.sh --cache-options /OMERO/BioFormatsCache.new --jobs 4 --batch-size 1000

`--db` allows to specify a custom database connection string instead of reading
it from the configuration file:

    ./regen-memo-files.sh --cache-options /OMERO/BioFormatsCache.new --jobs 4 --db postgresql://user:password@host:port/db

Checking the status
-------------------

To check the progress, use the `memofile-regen-status.sh` utility:

    $ ./memofile-regen-status.sh rslt.20250120.288193/
    Output Files / Total Images
    	3797/3797	100.00 %
    OK images / # of processed images
    	3686/3797	97.00 %
    OK images / # of total images
    	3686/3797	97.00 %
    Fail images / # of processed images
    	1/3797	0 %
    Fail images / # of total images
    	1/3797	0 %
    Skip images / # of processed images
    	110/3797	2.00 %
    Skip images / # of total images
    	110/3797	2.00 %

    Run Completion time: 0d 0h 32m 39s

Parallel timings tend to be off since a given image processes in wildly varying
amounts of time, but the whole process is parallelized based on the number of
CPUS/jobs.
`memofile-regen-status` must be run from above the rslt directory as it relies on relative locations to calculate it's statistics.


Output Files / Total Images is the overall progress.
Processed Images have 3 states, OK (memofile was successfully generated), Skip (memofile did not need to be regenerated but otherwise was fine), and Fail (something failed with the memofile regenration).

Failures do not halt the total regeneration process, in general failures are usually due to data being removed from the filesystem but not from the OMERO database.  Large numbers of failures should be investigated but low percentages can generally be accepted.

Parallel creates 3 files for each job it starts, a `seq` file that is a sequence numbers, `stdout` and `stderr`.  The memoizer java program will output the status (OK, Skip, Fail) and timings (if appropriate) along with the image id to `stdout`.  Any errors will be outputted to `stderr`.  So to check the first jobs's error output you would look at `rslt.20250120.288193/1/input.000/stderr`.  Jobs are based on their input file so 000-999 depending on the number of images and the batch size.

Development Installation
========================

1. Clone the repository::

        git clone https://github.com/glencoesoftware/omero-ms-image-region.git

1. Run the Gradle build and utilize the artifacts as required::

        ./gradlew installDist
        cd build/install
        ...

1. Log in to the OMERO.web instance you would like to develop against with
your web browser and with the developer tools find the `sessionid` cookie
value. This is the OMERO.web session key.

1. Run single or multiple image region tests using `curl`::

        curl -H 'Cookie: sessionid=<omero_web_session_key>' \
            http://localhost:8081/webgateway/render_image/<image_id>/<z>/<t>/

        curl -H 'Cookie: sessionid=<omero_web_session_key>' \
            http://localhost:8081/webgateway/render_image_region/<image_id>/<z>/<t>/?tile=0,0,0,1024,1024

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
