# Cross-compile in the build stage: every target architecture builds natively
# on the build platform, so multi-arch image builds stay fast.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X main.version=$VERSION" -o /ssefy .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ssefy /ssefy
EXPOSE 8080
ENTRYPOINT ["/ssefy"]
