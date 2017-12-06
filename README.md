# mytardis-nifcert
Verify NIF certified data; MyTardis, Celery task, Django application

# Development and contributions

To use the latest production MyTardis Docker image to base the code against perform the following:

1. Install [Docker](https://docs.docker.com/engine/installation/) and [Compose](https://docs.docker.com/compose/install/). Add your user account to the docker group so that you can control the docker daemon without having to run the instructions as a root user.
2. Create a development directory.
   - Inside the development directory add a src/ directory.
   - git clone any source code you are working on in relation to your development into your src/ directory.
   - Do not use git sub-modules as this Git feature is not for actively working on code.
3. Copy or symlink (preferred) the docker-compose.yml to the root of your development directory and alter the yaml "volumes:" array to bind mount your code into the correct location inside the docker image.
4. From your development directory issue the 'docker-compose up -d' command.
   - This will download the appropriate Docker images and start the services in Development mode to begin and test your new code.

Directory structure:

```
- Dev/
 |- src/
 | |- nifcert/         ; git clone https://github.com/UWA-FoS/mytardis-nifcert nifcert
 |- docker-compose.yml ; ln ./src/nifcert/docs/docker-compose.yml
```
Basic work example:

```
docker-compose exec django python manage.py shell
>>> from nifcert.tasks import add
>>> res = add.delay(2,2)
>>> res.id
>>> res.get(timeout=1)
```

This example starts the django shell within the django Docker container, loads the task method that is to be tested, submits the task to the message queue and then requests the returned value with a timeout of 1 second. Logged outputs for both Django and Celery can be viewed using the commands bellow.

```
docker-compose logs celery
docker-compose logs django
```
