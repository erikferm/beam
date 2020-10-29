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

// Package direct contains the direct runner for running single-bundle
// pipelines in the current process. Useful for testing.
package direct

import (
	"context"

	jman "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipeline "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

type JobService struct {
}

func (j *JobService) Prepare(ctx context.Context, prepareRequest *jman.PrepareJobRequest) (*jman.PrepareJobResponse, error) {

	return &jman.PrepareJobResponse{
		PreparationId:           prepareRequest.JobName,
		ArtifactStagingEndpoint: &pipeline.ApiServiceDescriptor{Url: "localhost:4444"},
		StagingSessionToken:     "token",
	}, nil
}

func (j *JobService) Run(context.Context, *jman.RunJobRequest) (*jman.RunJobResponse, error) {
	panic("not implemented")
}

func (j *JobService) GetJobs(context.Context, *jman.GetJobsRequest) (*jman.GetJobsResponse, error) {
	panic("not implemented")
}

func (j *JobService) GetState(context.Context, *jman.GetJobStateRequest) (*jman.JobStateEvent, error) {
	panic("not implemented")
}

func (j *JobService) GetPipeline(context.Context, *jman.GetJobPipelineRequest) (*jman.GetJobPipelineResponse, error) {
	panic("not implemented")
}

func (j *JobService) Cancel(context.Context, *jman.CancelJobRequest) (*jman.CancelJobResponse, error) {
	panic("not implemented")
}

func (j *JobService) GetStateStream(*jman.GetJobStateRequest, jman.JobService_GetStateStreamServer) error {
	panic("not implemented")
}

func (j *JobService) GetMessageStream(*jman.JobMessagesRequest, jman.JobService_GetMessageStreamServer) error {
	panic("not implemented")
}

func (j *JobService) GetJobMetrics(context.Context, *jman.GetJobMetricsRequest) (*jman.GetJobMetricsResponse, error) {
	panic("not implemented")
}

func (j *JobService) DescribePipelineOptions(context.Context, *jman.DescribePipelineOptionsRequest) (*jman.DescribePipelineOptionsResponse, error) {
	panic("not implemented")
}
