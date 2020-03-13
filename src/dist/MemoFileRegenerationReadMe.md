## Requirements

* GNU parallel
* omero-ms-image-region

## Running

The included wrapper script `regen-memo-files.sh` is used to wrap the image list
generation process, the running of parallel and the running of the memoizer into
a single step.

`regen-memo-files.sh --help` will display all the available options.

Common options are to specify a db connect string if different from the default

### Example

```
./regen-memo-files.sh --memoizer-home /opt/omero/omero-ms-image-region --cache-options /data/omero/memfiles.new/ --no-ask --no-wait --jobs 4

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
```

### Caveats

You can run your system out of memory by setting jobs too high.

It is also possible for the OOM killer to kill the job if there are too many running.
For example: when testing done on a 16G/8cpu system the parallel jobs will be
OOM'ed when jobs are set to >4.
