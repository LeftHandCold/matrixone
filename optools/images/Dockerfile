FROM matrixorigin/golang:1.24-ubuntu22.04 AS builder

# goproxy - use multiple Chinese mirrors for better reliability
# Set GOPROXY with multiple fallback options
ARG GOPROXY="https://goproxy.cn,https://proxy.golang.com.cn,https://mirrors.aliyun.com/goproxy/,direct"
ENV GOPROXY=${GOPROXY}
ENV GOSUMDB=sum.golang.google.cn
RUN echo "Setting GOPROXY to: ${GOPROXY}" && \
    go env -w GOPROXY="${GOPROXY}" && \
    go env -w GOSUMDB=sum.golang.google.cn && \
    go env -w GOPRIVATE="" && \
    echo "Verifying Go proxy settings:" && \
    go env | grep -E "(GOPROXY|GOSUMDB)" && \
    echo "Confirmed: GOPROXY is set to use Chinese mirrors"

RUN mkdir -p /go/src/github.com/matrixorigin/matrixone

WORKDIR /go/src/github.com/matrixorigin/matrixone

COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
# Add retry mechanism for network issues
RUN for i in 1 2 3 4 5; do \
        go mod download && break || \
        (echo "Retry $i failed, waiting..." && sleep $((i*2))); \
    done || exit 1

COPY . .

RUN make clean && make build

FROM matrixorigin/ubuntu:22.04

COPY --from=builder /go/src/github.com/matrixorigin/matrixone/mo-service /mo-service
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/etc /etc
COPY --from=builder /go/src/github.com/matrixorigin/matrixone/thirdparties/install/lib/*.so /usr/local/lib

# ldconfig and run mo-service to check if the shared library is found
RUN ldconfig && /mo-service -h

WORKDIR /

EXPOSE 6001

ENTRYPOINT [ "/mo-service", "-debug-http=:12345", "-launch", "/etc/quickstart/launch.toml"]
