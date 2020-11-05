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

// Package portable contains the portable runner for running beam pipelines
package portable

import (
	"context"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/options/jobopts"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal"
)

func init() {
	beam.RegisterRunner("portable", Execute)
}

const (
	defaultEndpoint = "localhost:4444"
)

// Execute runs the given pipeline on the portable runner. Convenience wrapper over the
// universal runner.
func Execute(ctx context.Context, p *beam.Pipeline) error {

	endpoint, err := jobopts.GetEndpoint()
	if err != nil {
		log.Info(ctx, "No endpoint specified. Using deafult endpoint: ", defaultEndpoint)
		endpoint = defaultEndpoint
		jobopts.Endpoint = &endpoint
	}

	// Start a JobServiceServer
	js := NewJobService(endpoint)
	go js.Start()

	return universal.Execute(ctx, p)
}
