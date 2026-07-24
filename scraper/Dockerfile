# ---- build stage: has the Go compiler ----
FROM golang:1.22-alpine AS build
WORKDIR /src

# Copy dependency files first. Docker caches layers, so deps only re-download
# when go.mod/go.sum change — not on every code edit.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 makes a fully static binary with no OS library dependencies,
# which is what lets us use an almost-empty final image.
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/scraper .

# ---- runtime stage: just the binary ----
FROM gcr.io/distroless/static-debian12
COPY --from=build /bin/scraper /bin/scraper
EXPOSE 8080
ENTRYPOINT ["/bin/scraper"]