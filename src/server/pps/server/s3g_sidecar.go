package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
	logrus "github.com/sirupsen/logrus"
)

type sidecarS3G struct {
	apiServer    *apiServer
	pipelineInfo *pps.PipelineInfo
}

func (a *apiServer) ServeSidecarS3G() error {
	s := &sidecarS3G{
		apiServer:    a,
		pipelineInfo: &pps.PipelineInfo{}, // populate below
	}

	// Read spec commit for this sidecar's pipeline
	specCommit := a.env.PPSSpecCommitID
	if specCommit == "" {
		return errors.New("cannot serve sidecar S3 gateway if no spec commit is set")
	}
	if err := backoff.Retry(func() error {
		retryCtx, retryCancel := context.WithCancel(context.Background())
		defer retryCancel()
		return a.sudo(a.env.GetPachClient(retryCtx), func(superUserClient *client.APIClient) error {
			buf := bytes.Buffer{}
			if err := superUserClient.GetFile(ppsconsts.SpecRepo, specCommit, ppsconsts.SpecFile, 0, 0, &buf); err != nil {
				return fmt.Errorf("could not read existing PipelineInfo from PFS: %v", err)
			}
			if err := s.pipelineInfo.Unmarshal(buf.Bytes()); err != nil {
				return fmt.Errorf("could not unmarshal PipelineInfo bytes from PFS: %v", err)
			}
			return nil
		})
	}, backoff.New10sBackOff()); err != nil {
		return fmt.Errorf("error starting sidecar s3 gateway: %v", err)
	}
	if !ppsutil.ContainsS3Inputs(s.pipelineInfo.Input) && !s.pipelineInfo.S3Out {
		return nil // nothing to serve via S3 gateway
	}

	return s.Serve()
}

func (s *sidecarS3G) Serve() error {
	return backoff.RetryNotify(func() (retErr error) {
		// Watch for new jobs & initialize s3g for each new job
		retryCtx, retryCancel := context.WithCancel(context.Background())
		defer retryCancel()
		watcher, err := s.apiServer.jobs.ReadOnly(retryCtx).WatchByIndex(ppsdb.JobsPipelineIndex, s.pipelineInfo.Pipeline)
		if err != nil {
			return fmt.Errorf("error creating watch: %v", err)
		}
		defer watcher.Close()
		for e := range watcher.Watch() {
			if e.Type == watch.EventError {
				return fmt.Errorf("sidecar s3 gateway watch error: %v", e.Err)
			} else if e.Type == watch.EventDelete {
				// Job was deleted, e.g. because input commit was deleted. This is
				// handled by cancelCtxIfJobFails goro, which was spawned when job was
				// created. Nothing to do here
				continue
			}

			// 'e' is a Put event -- new job
			var jobID string
			jobPtr := &pps.EtcdJobInfo{}
			if err := e.Unmarshal(&jobID, jobPtr); err != nil {
				return fmt.Errorf("error unmarshalling: %v", err)
			}
			if ppsutil.IsTerminal(jobPtr.State) {
				// previously-created job has finished, or job was finished during backoff
				// or in the 'watcher' queue
				logrus.Infof("skipping job %v as it is already in state %v", jobID, jobPtr.State)
				continue
			}
		}

		// Initialize new S3 gateway
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("worker: watch closed or error running the worker process: %v; retrying in %v", err, d)
		return nil
	})
}
