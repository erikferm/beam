// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package portable

import (
	"bytes"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipeline "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"os"
)

const (
	DefaultArtifactEndpoint = "localhost:4445"
)

type ArtifactStagingService struct {
	Endpoint  string
	Token     string
	Artifacts map[string][]*pipeline.ArtifactInformation
}

func (a *ArtifactStagingService) start() <-chan error {
	out := make(chan error)
	lis, err := net.Listen("tcp", a.Endpoint)
	if err != nil {
		out <- err
		return out
	}
	server := grpc.NewServer()
	jobpb.RegisterArtifactStagingServiceServer(server, a)
	go func(server *grpc.Server, lis net.Listener) {
		if err := server.Serve(lis); err != nil {
			out <- err
		}
	}(server, lis)
	return out

}

func NewArtifactStagingService() *ArtifactStagingService {
	artifacts := make(map[string][]*pipeline.ArtifactInformation)
	service := &ArtifactStagingService{
		Endpoint:  DefaultArtifactEndpoint,
		Artifacts: artifacts,
	}
	// TODO: What should we do with errors?
	go service.start()
	return service
}

func (a *ArtifactStagingService) RegisterArtifactsWithToken(token string, artifactInfo []*pipeline.ArtifactInformation) {
	a.Artifacts[token] = artifactInfo
}

func (a *ArtifactStagingService) ReverseArtifactRetrievalService(srv jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	ctx, cancel := context.WithCancel(srv.Context())
	// Wait for a staging token
	respWrapper, err := srv.Recv()
	if err != nil {
		return err
	}
	stagingToken := respWrapper.StagingToken

	// Check if there is artifact information registered for the token
	artifacts, ok := a.Artifacts[stagingToken]
	if !ok {
		/*
			TODO: Figure out how to send this to the client.
			Is it really necessary to create a StreamingInterceptor to achieve this?
		*/
		return status.New(codes.NotFound, "No such token").Err()
	}

	// Ask the client to stage each artifact registered with the token
	for i, artifactInfo := range artifacts {
		w, err := createArtifactWriter(fmt.Sprintf("%s-%s", stagingToken, i))
		if err != nil {
			return err
		}
		// Request artifact from client
		if err := requestArtifact(ctx, artifactInfo, srv);err!= nil {
			return err
		}
		// Write artifact to service storage
		if err := stageArtifact(ctx, w, srv); err != nil {
			return err
		}
	}

	log.Debug(ctx, "Done staging artifacts. Closing stream")
	// Cancel the server context to signal end of stream to client
	cancel()
	return nil
}

func requestArtifact(ctx context.Context, artifactInfo *pipeline.ArtifactInformation, srv jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	artifactRequest := &jobpb.ArtifactRequestWrapper{
		Request: &jobpb.ArtifactRequestWrapper_GetArtifact{
			GetArtifact: &jobpb.GetArtifactRequest{
				Artifact: artifactInfo,
			},
		},
	}
	return srv.Send(artifactRequest)
}

func stageArtifact(ctx context.Context, w io.WriteCloser, srv jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	// Receive chunks for an artifact
	for {
		respWrapper, err := srv.Recv()
		if err != nil {
			return err
		}
		if respWrapper.IsLast {
			return w.Close()
		}
		log.Debug(ctx, "Staging chunck for: ", respWrapper.StagingToken)
		if _, err := io.Copy(w, bytes.NewReader(respWrapper.Response.(*jobpb.ArtifactResponseWrapper_GetArtifactResponse).GetArtifactResponse.Data)); err != nil {
			return err
		}
	}
}

func createArtifactWriter(id string) (io.WriteCloser, error) {
	return os.Create(fmt.Sprintf("artifacts/%s", id))
}
