FROM ubuntu:16.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install curl -y

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y nodejs

COPY . .

ENTRYPOINT [ "node", "app.js" ]