#!/bin/sh


echo "Building base image"
docker build . -f base-image.Dockerfile -t base_image:latest

cd darling
echo "Building DARLING's image"

docker build . -f Dockerfile -t darling:latest
echo "Running processor container"
docker run -it -d --name darling-processor --env-file ./env --env IP=$1 --env PORT=$2 -p "$2:80" --cpus="1" --memory="4g" darling:latest python -u /cep/processor.py
echo "Running generator container"
docker run -it -d --name darling-generator --env-file ./env --env IP=$1 --env PORT=$2 --cpus="1" --memory="4g" darling:latest python -u /cep/generator.py

cd ../espice
echo "Building eSPICE's image"

docker build . -f Dockerfile -t espice:latest
echo "Running processor container"
docker run -it -d --name espice-processor --env-file ./env --env IP=$1 --env PORT=$3 -p "$3:80" --cpus="1" --memory="4g" espice:latest python -u /cep/processor.py
echo "Running generator container"
docker run -it -d --name espice-generator --env-file ./env --env IP=$1 --env PORT=$3 --cpus="1" --memory="4g" espice:latest python -u /cep/generator.py

cd ../hspice
echo "Building hSPICE's image"

docker build . -f Dockerfile -t hspice:latest
echo "Running processor container"
docker run -it -d --name hspice-processor --env-file ./env --env IP=$1 --env PORT=$4 -p "$4:80" --cpus="1" --memory="4g" hspice:latest python -u /cep/processor.py
echo "Running generator container"
docker run -it -d --name hspice-generator --env-file ./env --env IP=$1 --env PORT=$4 --cpus="1" --memory="4g" hspice:latest python -u /cep/generator.py


cd ../icde20
echo "Building ICDE'20 image"

docker build . -f Dockerfile -t icde:latest
echo "Running processor container"
docker run -it -d --name icde20-processor --env-file ./env --env IP=$1 --env PORT=$5 -p "$5:80" --cpus="1" --memory="4g" icde:latest python -u /cep/processor.py
echo "Running generator container"
docker run -it -d --name icde20-generator --env-file ./env --env IP=$1 --env PORT=$5 --cpus="1" --memory="4g" icde:latest python -u /cep/generator.py


docker ps
