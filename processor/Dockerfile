FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/processor .

FROM gcr.io/distroless/static-debian12
COPY --from=build /bin/processor /bin/processor
EXPOSE 8081
ENTRYPOINT ["/bin/processor"]