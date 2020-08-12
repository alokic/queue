# queue
[![Coverage Status](https://coveralls.io/repos/github/alokic/queue/badge.svg?branch=master&t=6g0laG)](https://coveralls.io/github/alokic/queue?branch=master)

`queue` is job processing service which can be embedded as sidecar. 

As of now it supports kafka. Plan is to add redis and kinesis.

It has 2 components:
controller and dispatcher.

Controller is for registering job config. Job config defines address of broker, queue name (topics) etc.
Dispatcher syncs job config from controller.

Dispatcher can be run as a sidecar container in your pod OR as a standalone service.

# Local setup
queue involves a lot of components namely controller, dispatcher, Postgres, kafka broker, zookeeper.
In order to test the worker locally, you have to run all services.
To this end, there is a docker compose for spwaning all services.

`Prerequisites`
- Download docker and docker-compose.
- Get access to honestbee quay repo and read permissions on alokic/queue-controller and alokic/queue-dispatcher repos. 
- Login to quay:

  `$docker login quay.io`
     

The steps are below:
- Git clone https://github.com/alokic/queue to /tmp/queue.  
- cd to /tmp/queue
- the ports(5432,2181,9092,4000,4001) on the host should be free.
- docker-compose up(you can also run in background with -d flag)
- in a separate terminal do docker-compose ps and see status.


## Release Process

We follow [Feature branch workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow)

Feature branches are merged in `master` branch after `PR` approval.

Deployment is triggered when we tag `master` branch. This can be done from cmdline by creating releases and pushing it OR ideally from github.

Tagging convention for `production` environment is `vMAJOR_NUMBER.MINOR_NUMBER.PATCH_NUMBER`.

We increment `NUMBERS` when:

* `MAJOR_NUMBER` when breaking backward compatibility.
* `MAJOR_NUMBER` when adding a new feature which doesn’t break compatibility.
* `PATCH_NUMBER` when fixing a bug without breaking compatibility.

Tagging convention for `staging` is `vMAJOR_NUMBER.MINOR_NUMBER.PATCH_NUMBER-<SUFFIX>.<CHILD_SUFFIX>`.

* `SUFFIX` is mandatory for `staging` deployments. Valid `SUFFIX` are `alpha`, `beta`, `rc`.
* `CHILD_SUFFIX` is optional and should be used for incremental updates in a tag like `v1.0.0-rc.1`

# Release Steps
- Go to Releases tab.
- Create a new alpha/beta/rc on master branch. This would deploy to staging and run migration.
- Create a release and this would deploy and run migration on prod.
