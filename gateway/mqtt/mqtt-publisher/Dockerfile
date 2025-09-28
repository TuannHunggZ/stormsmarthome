FROM node:18-slim

RUN mkdir -p /app/

WORKDIR /app/

COPY package.json .

RUN npm install

COPY index.js .
