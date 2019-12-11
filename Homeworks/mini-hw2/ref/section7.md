# Installation instructions

We recommend installing Vertica via a precompiled Docker image.  If you don't already have Docker installed, a good option is to launch an EC2 instance and run it there.  On the other hand, Docker is fantastic and having the ability to use it locally is great.

## 1. Install Docker

(*If you already have Docker installed, you can skip to Step 2.*)

Docker is available for most major platforms, including Windows, Mac and Linux.
Installation instructions can be found [here](https://docs.docker.com/engine/getstarted/step_one/).
For those interested in learning more, the [Docker Docs](https://docs.docker.com) are a good
resource, especially this [Overview](https://docs.docker.com/engine/understanding-docker/).
This is also a nice [command line reference](https://docs.docker.com/engine/reference/commandline/docker/).

If you're running Ubuntu (possibly on an EC2 instance), you can install Docker via Aptitude:

```sh
sudo apt update
sudo apt install docker.io
```

## 2. Download and run the Vertica image

Vertica is available on [Docker Hub](https://hub.docker.com/), which is a public
registry of Docker repositories. There are several Vertica repositories available,
but we will be using [this one](https://hub.docker.com/r/sumitchawla/vertica/).
The full installation instructions can be found by following the link, but the following
steps should suffice.

*Depending on how you installed Docker, you may need to run the following commands using `sudo`*

### Retrieve the image:
```sh
docker pull sumitchawla/vertica
```

### Launch a container

The `-p` flag maps host port 5433 to container port 5433, which is the default Vertica port.

```
docker run -d -p 5433:5433 sumitchawla/vertica
```

You can check the status of running containers by issuing:
```
docker ps
```

You should see that your Vertica container is now running as an active container.

#### Connection parameters:
- Default DB Name: `docker`
- Default User: `dbadmin`
- Default Password: (None)

### 3. Install the clients

#### Vertica Python Library

There are several different clients available to communicate with a Vertica database that should now be running on your computer on port 5433.  

Perhaps the easiest to use is Vertica's Python library.
This client library is available on [Github](https://github.com/uber/vertica-python) and can be installed through a simple `pip` installation.
Note that some functionality of the client depends on the Python PostgrSQL library, `psycopg2`, which you may need to install as well (if you don't have it already):

```
apt install python-pip # If not already installed

pip install vertica-python
```

*Using the Vertica Client Library:*

Basic usage of the Python client is demonstrated in [sample.py](https://courses.cs.washington.edu/courses/csed516/19au/sections/sample.py).  Try running
this script to make sure everything works. Compare the output of the script
with the [sample_data.csv](https://courses.cs.washington.edu/courses/csed516/19au/sections/sample_data.csv) file:

```sh
wget https://courses.cs.washington.edu/courses/csed516/19au/sections/sample.py
wget https://courses.cs.washington.edu/courses/csed516/19au/sections/sample_data.csv

python sample.py
```

#### Interactive shell: `vsql`

The vsql client is almost identical to the psql client for Postgres. [Go here](https://my.vertica.com/download/vertica/client-drivers/) 
to download the vsql client appropriate to your system, or issue the following commands if you're running on Ubuntu:

```sh
wget https://www.vertica.com/client_drivers/9.3.x/9.3.0-0/vertica-client-9.3.0-0.x86_64.tar.gz
tar xzf vertica-client-9.3.0-0.x86_64.tar.gz
```

Otherwise, if you are using the Docker image from above, download client drivers version 9.0.x. On OSX the download package will contain a binary `vsql` --- copy it into your working directory (preferably this one) and try to run it with `./vsql`. 

To connect to a running Vertica database (like the one you've installed above), be sure that the database is running and connect with 
```
cd opt/vertica/bin # Or wherever vsql is located
./vsql -d docker dbadmin
```

You can then execute queries interactively. Try querying the table you ingested via the `sample.py` script:
```
select * from sample_table;
```

### Timing Queries using VSQL

This can be done by either turning timing on using the flag `\timing` or
by querying the relation `v_monitor.query_requests` (using `\timing` is the easiest!).

```sql
select  request_duration_ms, request  from v_monitor.query_requests limit 10;
```
