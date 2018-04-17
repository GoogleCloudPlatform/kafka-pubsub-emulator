/**
*  Copyright 2018 Google LLC
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      https://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package main

import (
	"flag"
	"net/http"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	gwAdmin "./internal/google.golang.org/genproto/googleapis/pubsub/kafka/emulator/v1"
	gwHealth "./internal/google.golang.org/genproto/googleapis/health/v1"
	gwPubsub "./internal/google.golang.org/genproto/googleapis/pubsub/v1"
	"strings"
	"path"

	"github.com/spf13/cobra"
	"fmt"
	"os"
	"google.golang.org/grpc/credentials"
)

var (
	swaggerDir = flag.String("swagger_dir", "./internal/swagger/proto", "path to the directory which contains swagger definitions")
)

var port, address, certFile string

func newGateway(ctx context.Context, opts ...runtime.ServeMuxOption) (http.Handler, error) {
	var (
		adminEndpoint      = flag.String("admin_endpoint", address, "endpoint of Admin")
		healthEndpoint     = flag.String("health_endpoint", address, "endpoint of Health")
		publisherEndpoint  = flag.String("publisher_endpoint", address, "endpoint of Publisher")
		subscriberEndpoint = flag.String("subscriber_endpoint", address, "endpoint of Subscriber")
	)

	mux := runtime.NewServeMux(opts...)

	dialOpts := []grpc.DialOption{dialOption()}

	err := gwAdmin.RegisterAdminHandlerFromEndpoint(ctx, mux, *adminEndpoint, dialOpts)
	if err != nil {
		return nil, err
	}

	err = gwHealth.RegisterHealthHandlerFromEndpoint(ctx, mux, *healthEndpoint, dialOpts)
	if err != nil {
		return nil, err
	}

	err = gwPubsub.RegisterPublisherHandlerFromEndpoint(ctx, mux, *publisherEndpoint, dialOpts)
	if err != nil {
		return nil, err
	}

	err = gwPubsub.RegisterSubscriberHandlerFromEndpoint(ctx, mux, *subscriberEndpoint, dialOpts)
	if err != nil {
		return nil, err
	}
	return mux, nil
}

func dialOption() grpc.DialOption {
	if len(strings.TrimSpace(certFile)) == 0 {
		return grpc.WithInsecure()
	}
	creds, err := credentials.NewClientTLSFromFile(certFile, "")
	if err != nil {
		glog.Fatalf("Failed to create TLS credentials %v", err)
	}
	return grpc.WithTransportCredentials(creds)
}

func serveSwagger(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, ".swagger.json") {
		glog.Errorf("Not Found: %s", r.URL.Path)
		http.NotFound(w, r)
		return
	}
	glog.Infof("Serving %s", r.URL.Path)
	p := strings.TrimPrefix(r.URL.Path, "/swagger/")
	p = path.Join(*swaggerDir, p)
	preflightHandler(w, r)
	http.ServeFile(w, r, p)
}

// allowCORS allows Cross Origin Resource Sharing from any origin.
// Don't do this without consideration in production systems.
func allowCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		preflightHandler(w, r)
		h.ServeHTTP(w, r)
	})
}

// Don't do this without consideration in production systems.
func preflightHandler(w http.ResponseWriter, r *http.Request) {
	headers := []string{"Content-Type", "Accept"}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", strings.Join(headers, ","))
	methods := []string{"GET", "HEAD", "POST", "PUT", "DELETE"}
	w.Header().Set("Access-Control-Allow-Methods", strings.Join(methods, ","))
	glog.Infof("preflight request for %s", r.URL.Path)
}

// Run starts a HTTP server and blocks forever if successful.
func Run(address string, opts ...runtime.ServeMuxOption) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := http.NewServeMux()
	mux.HandleFunc("/swagger/", serveSwagger)

	gw, err := newGateway(ctx, opts...)
	if err != nil {
		return err
	}
	mux.Handle("/", gw)

	return http.ListenAndServe(address, allowCORS(mux))
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "start",
		Short: "Start GRPC gateway server for kafka-pubsub-emulator REST Endpoints.",
		Run: func(cmd *cobra.Command, args []string) {
			flag.Parse()
			defer glog.Flush()

			if err := Run(fmt.Sprintf(":%s", port)); err != nil {
				glog.Fatal(err)
			}
		},
	}

	rootCmd.Flags().StringVarP(&port, "port", "p", "8181", "port of kafka-pubsub-emulator-gateway.")
	rootCmd.Flags().StringVarP(&address, "address", "a", "", "kafka pubsub emulator address. (required)")
	rootCmd.Flags().StringVarP(&address, "cert_file_path", "c", "", "cert file path to authenticate with kafka pubsub emulator.")
	rootCmd.MarkFlagRequired("address")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
