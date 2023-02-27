FROM golang:alpine

MAINTAINER Maintainer

###############################################################################
#                                INSTALLATION
###############################################################################

WORKDIR /app/main

ENV PORT=8787
ENV GOPROXY=https://goproxy.cn

COPY ./ $WORKDIR

RUN go get go-redis && go build go-redis

EXPOSE $PORT

ENTRYPOINT ["./go-redis"]

