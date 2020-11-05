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
	"context"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipeline "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"net"
)

type BeamJob struct {
	ID                     string
	Pipeline               *pipeline.Pipeline
	ArtifactInfo           []*pipeline.ArtifactInformation
	ArtifactStagingService *ArtifactStagingService
}

type JobService struct {
	Endpoint               string
	Jobs                   map[string]*BeamJob
	ArtifactStagingService *ArtifactStagingService
}

func NewJobService(endpoint string) *JobService {
	return &JobService{
		Endpoint:               endpoint,
		Jobs:                   make(map[string]*BeamJob),
		ArtifactStagingService: NewArtifactStagingService(),
	}
}

func (j *JobService) Prepare(ctx context.Context, req *jobpb.PrepareJobRequest) (*jobpb.PrepareJobResponse, error) {
	jobId := fmt.Sprintf("%s-%s", req.JobName, uuid.New())
	stagingToken := jobId

	// Register all the artifacts for the job
	var artifactInfo []*pipeline.ArtifactInformation
	for _, env := range req.Pipeline.Components.Environments {
		artifactInfo = append(artifactInfo, env.Dependencies...)
	}
	j.ArtifactStagingService.RegisterArtifactsWithToken(stagingToken,artifactInfo)

	// Create a new job
	j.Jobs[jobId] = &BeamJob{
		ID:                     jobId,
		Pipeline:               req.Pipeline,
		ArtifactInfo:           artifactInfo,
		ArtifactStagingService: j.ArtifactStagingService,
	}

	log.Debug(ctx, "Created new job: ", j.Jobs[jobId])

	return &jobpb.PrepareJobResponse{
		PreparationId:           jobId,
		ArtifactStagingEndpoint: &pipeline.ApiServiceDescriptor{Url: j.ArtifactStagingService.Endpoint},
		StagingSessionToken:     stagingToken,
	}, nil
}

func (j *JobService) Run(context.Context, *jobpb.RunJobRequest) (*jobpb.RunJobResponse, error) {
	panic("not implemented")
}

func (j *JobService) GetJobs(context.Context, *jobpb.GetJobsRequest) (*jobpb.GetJobsResponse, error) {
	panic("not implemented")
}

func (j *JobService) GetState(context.Context, *jobpb.GetJobStateRequest) (*jobpb.JobStateEvent, error) {
	panic("not implemented")
}

func (j *JobService) GetPipeline(context.Context, *jobpb.GetJobPipelineRequest) (*jobpb.GetJobPipelineResponse, error) {
	panic("not implemented")
}

func (j *JobService) Cancel(context.Context, *jobpb.CancelJobRequest) (*jobpb.CancelJobResponse, error) {
	panic("not implemented")
}

func (j *JobService) GetStateStream(*jobpb.GetJobStateRequest,
	jobpb.JobService_GetStateStreamServer) error {
	panic("not implemented")
}

func (j *JobService) GetMessageStream(*jobpb.JobMessagesRequest,
	jobpb.JobService_GetMessageStreamServer) error {
	panic("not implemented")
}

func (j *JobService) GetJobMetrics(context.Context, *jobpb.GetJobMetricsRequest) (*jobpb.GetJobMetricsResponse, error) {
	panic("not implemented")
}

func (j *JobService) DescribePipelineOptions(context.Context, *jobpb.DescribePipelineOptionsRequest) (*jobpb.DescribePipelineOptionsResponse, error) {
	panic("not implemented")
}

func (j *JobService) Start() <-chan error {
	out := make(chan error)
	lis, err := net.Listen("tcp", j.Endpoint)
	if err != nil {
		out <- err
		return out
	}
	server := grpc.NewServer()
	jobpb.RegisterJobServiceServer(server, j)
	go func(server *grpc.Server, lis net.Listener) {
		if err := server.Serve(lis); err != nil {
			out <- err
		}
	}(server, lis)
	return out
}

// GetClient is a convienience function for testing
func (j *JobService) getClient() (jobpb.JobServiceClient, error) {
	conn, err := grpc.Dial(j.Endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return jobpb.NewJobServiceClient(conn), nil
}
