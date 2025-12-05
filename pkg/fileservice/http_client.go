// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/ncruces/go-dns"
	"go.uber.org/zap"
)

var (
	connectTimeout   = time.Second * 5
	readWriteTimeout = time.Second * 20
	maxIdleConns     = 100
	// maxIdleConnsPerHost: 每个主机的最大空闲连接数
	// 应该与 maxConnsPerHost 保持一致，以充分利用连接池
	maxIdleConnsPerHost = 500
	// maxConnsPerHost: 每个主机的最大并发连接数
	// 如果并发任务数较多（如 100+），需要相应增加此值
	// 每个 Write 操作可能需要 2 个连接（Exists + Write），所以 100 个并发至少需要 200
	// 设置为 500 以支持更高的并发场景
	maxConnsPerHost = 500
	idleConnTimeout = 10 * time.Second
)

var dnsResolver = dns.NewCachingResolver(
	nil,
	dns.MaxCacheEntries(128),
)

func init() {
	net.DefaultResolver = dnsResolver
	http.DefaultTransport = httpRoundTripper
}

var httpDialer = &net.Dialer{
	Timeout:  connectTimeout,
	Resolver: dnsResolver,
}

var httpTransport = &http.Transport{
	DialContext:           wrapDialContext(httpDialer.DialContext),
	MaxIdleConns:          maxIdleConns,
	IdleConnTimeout:       idleConnTimeout,
	MaxIdleConnsPerHost:   maxIdleConnsPerHost,
	MaxConnsPerHost:       maxConnsPerHost,
	TLSHandshakeTimeout:   connectTimeout,
	ResponseHeaderTimeout: readWriteTimeout,
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
		RootCAs:            caPool,
	},
	Proxy: http.ProxyFromEnvironment,
}

func init() {
	// don't know why there is a large number of connections even though MaxConnsPerHost is set.
	// close idle connections periodically.
	go func() {
		for range time.NewTicker(time.Second).C {
			httpTransport.CloseIdleConnections()
		}
	}()
}

var httpRoundTripper = wrapRoundTripper(httpTransport)

var caPool = func() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		panic(err)
	}
	return pool
}()

func newHTTPClient(args ObjectStorageArguments) *http.Client {

	// custom certs
	if len(args.CertFiles) > 0 {
		// custom certs
		for _, path := range args.CertFiles {
			content, err := os.ReadFile(path)
			if err != nil {
				logutil.Info("load cert file error",
					zap.Any("err", err),
				)
				// ignore
				continue
			}
			logutil.Info("file service: load cert file",
				zap.Any("path", path),
			)
			caPool.AppendCertsFromPEM(content)
		}
	}

	// client
	client := &http.Client{
		Transport: httpRoundTripper,
	}

	return client
}
