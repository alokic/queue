FROM golang:1.11-alpine as builder

RUN apk update
RUN apk add --no-cache build-base sed git curl
RUN sed -i -e 's/v3\.8/edge/g' /etc/apk/repositories && apk add --no-cache librdkafka-dev=0.11.6-r1

ENV PATH /go/bin:/usr/local/go/bin:$PATH
ENV GOPATH /go
ENV BUILDTARGET dispatcher

WORKDIR $GOPATH/src/github.com/alokic/queue
COPY . .

RUN make ${BUILDTARGET}
###############################

FROM alpine:3.8 as dispatcher-image

RUN apk add --no-cache build-base
RUN sed -i -e 's/v3\.8/edge/g' /etc/apk/repositories && apk add --no-cache librdkafka-dev=0.11.6-r1

ENV GOPATH /go
ENV BUILDTARGET dispatcher

COPY --from=builder /go/bin/${BUILDTARGET} /usr/bin/${BUILDTARGET}
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs
COPY --from=builder /go/src/github.com/alokic/queue/config /go/src/github.com/alokic/queue/config

CMD [ "dispatcher" ]