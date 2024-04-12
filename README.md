# Green Access
Green Access is an experimental HPC-FaaS platform for "fungible" allocations, or allocations for running jobs across multiple allocations. Green Access implements **Carbon Based Accounting (CBA)** where the cost a running a job depends on the carbon footprint of that job. The Green-Access prototype consists of three python packages.

## Green Access Service

To install the green access service, clone the GreenAccess repository and install the
run `pip install .` inside the `service/` folder.

To start the servive, authenticate with diaspora-event-sdk and create topics for producing events. In a Python terminal, run:
from diaspora_event_sdk import Client
```
c = Client()
c.register_topic("ga-resources-<user>")
c.register_topic("ga-predictions-<user>")
```

Running those commands will require authentication with Globus Auth. Once authenticated, we create Kafka topics for passing resource information from the endpoints to the service. Update these topic names inside the `green-access-service/common.py` configuration file.

GreenAccess also relies on Redis and MySQL to store data. Those connection parameters can also be updated in `green-access-service/common.py`.

To start the web app run, `uvicorn router:app`. To start the faust consumer, run `faust -A monitor -l info worker`.

## Green Access Client

The Green Access client provides a Python client to interact with the Green Access service. To install the client, run `pip install .` inside of the `client/` folder.

## Green Access Experiments

The Green Access Experiments package was used to run experiments on the Green Access platform. It includes several benchmark functions, as well as the ability to simulate users who follow certain policies. To install the package, run `pip install .` inside the `experiments` folder.