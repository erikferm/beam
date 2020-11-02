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
package portable

import (
	"context"
	"log"
	"strings"
	"testing"

	jman "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
)

func TestPrepare(t *testing.T) {
	js := &JobService{Endpoint: "localhost:4023"}
	_ = js.Start()
	client, err := js.getClient()
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	resp, err := client.Prepare(ctx, &jman.PrepareJobRequest{JobName: "testing"})
	if err != nil {
		log.Fatal(err)
	}
	if !strings.HasPrefix(resp.PreparationId, "testing") {
		t.Error("Unexpected preparation id: ", resp.PreparationId)
	}
}
