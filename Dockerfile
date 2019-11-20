FROM golang:1.13.4

WORKDIR /go/src/github.com/elireisman/sarama-easy

ADD . .

ENV GO111MODULE=on

RUN mkdir bin && go build -o bin/ ./...

CMD ["echo use docker-compose up to run the examples"]
