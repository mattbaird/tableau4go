package tableau4go

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"
)

func getEnvOrElse(env_var, default_setting) {
	ret_val := os.Getenv(env_var)
	if len(ret_var) <= 0 {
		ret_val = default_setting
	}
}

var (
	connectTimeOut   = getEnvOrElse("tableau_connect_timeout", 10*time.Second)
	readWriteTimeout = getEnvOrElse("tableau_readwrite_timeout", 20*time.Second)
)

func timeoutDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, cTimeout)
		if err != nil {
			return nil, err
		}
		if rwTimeout > 0 {
			conn.SetDeadline(time.Now().Add(rwTimeout))
		}
		return conn, nil
	}
}

// apps will set two OS variables:
// atscale_http_sslcert - location of the http ssl cert
// atscale_http_sslkey - location of the http ssl key
func NewTimeoutClient(cTimeout time.Duration, rwTimeout time.Duration, useClientCerts bool) *http.Client {
	certLocation := os.Getenv("atscale_http_sslcert")
	keyLocation := os.Getenv("atscale_http_sslkey")
	caFile := os.Getenv("atscale_ca_file")
	// default
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	if useClientCerts && len(certLocation) > 0 && len(keyLocation) > 0 {
		// Load client cert if available
		cert, err := tls.LoadX509KeyPair(certLocation, keyLocation)
		if err == nil {
			if len(caFile) > 0 {
				caCertPool := x509.NewCertPool()
				caCert, err := ioutil.ReadFile(caFile)
				if err != nil {
					fmt.Printf("Error setting up caFile [%s]:%v\n", caFile, err)
				}
				caCertPool.AppendCertsFromPEM(caCert)
				tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true, RootCAs: caCertPool}
				tlsConfig.BuildNameToCertificate()
			} else {
				tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
			}
		}
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
			Dial:            timeoutDialer(cTimeout, rwTimeout),
		},
	}
}

func DefaultTimeoutClient() *http.Client {
	return NewTimeoutClient(connectTimeOut, readWriteTimeout, false)
}
