FROM golang:1.11-alpine as builder

RUN apk update
RUN apk add --no-cache build-base git

ENV PATH /go/bin:/usr/local/go/bin:$PATH
ENV GOPATH /go
ENV BUILDTARGET controller

WORKDIR $GOPATH/src/github.com/alokic/queue
COPY . .

RUN make ${BUILDTARGET}
###############################

FROM alpine:3.8 as controller-image

RUN apk add --no-cache build-base tree

ENV GOPATH /go
ENV BUILDTARGET controller

COPY --from=builder /go/bin/${BUILDTARGET} /usr/bin/${BUILDTARGET}
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs
COPY --from=builder /go/src/github.com/alokic/queue/config /go/src/github.com/alokic/queue/config

CMD [ "controller" ]