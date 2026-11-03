FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /ssefy .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ssefy /ssefy
EXPOSE 8080
ENTRYPOINT ["/ssefy"]
