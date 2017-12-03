# mytardis-nifcert
Verify NIF certified data; MyTardis, Django application, Celery task

# Development and contributions

To use the latest production Docker image to base the code against perform the following:

1. Install [Docker](https://docs.docker.com/engine/installation/) and [Compose](https://docs.docker.com/compose/install/). Add your user account to the docker group so that you can control the docker daemon without having to run the instructions as a root user.
2. Create a development directory.
2.1. Inside the development directory add a src/ directory and git clone any source code you are working on in relation to your development into your src/ directory. Do not use git sub-modules as this Git feature is not for actively working on code.
3. Copy the docker-compose.yml to the root of your development directory and alter the yaml "volumes:" array to bind mount your code into the correct location inside the docker image.
4. From your development directory issue the 'docker-compose up -d' command. This will download the appropriate Docker images and start the services in Debug mode to begin and test your new code.