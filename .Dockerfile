FROM golang:1.13-alpine

WORKDIR /home

COPY . .

RUN go build main.go

ENTRYPOINT ["./main"]
