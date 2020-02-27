package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pps"
	"github.com/pachyderm/pachyderm/src/server/pfs/s3"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsconsts"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsdb"
	"github.com/pachyderm/pachyderm/src/server/pkg/ppsutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/watch"
	logrus "github.com/sirupsen/logrus"
)

type sidecarS3G struct {
	apiServer    *apiServer
	pipelineInfo *pps.PipelineInfo
	pachClient   *client.APIClient

	serversMu sync.Mutex
	servers   map[string]*http.Server
}

func (a *apiServer) ServeSidecarS3G() (retErr error) {
	defer func() {
		panic(
			fmt.Sprintf("sidecar s3 gateway is exiting; this should never happen (err: %v)", retErr),
		)
	}()
	s := &sidecarS3G{
		apiServer:    a,
		pipelineInfo: &pps.PipelineInfo{}, // populate below
		pachClient:   a.env.GetPachClient(context.Background()),
	}

	// Read spec commit for this sidecar's pipeline, and set auth token for pach
	// client
	specCommit := a.env.PPSSpecCommitID
	if specCommit == "" {
		return errors.New("cannot serve sidecar S3 gateway if no spec commit is set")
	}
	if err := backoff.Retry(func() error {
		retryCtx, retryCancel := context.WithCancel(context.Background())
		defer retryCancel()
		if err := a.sudo(s.pachClient.WithCtx(retryCtx), func(superUserClient *client.APIClient) error {
			buf := bytes.Buffer{}
			if err := superUserClient.GetFile(ppsconsts.SpecRepo, specCommit, ppsconsts.SpecFile, 0, 0, &buf); err != nil {
				return fmt.Errorf("could not read existing PipelineInfo from PFS: %v", err)
			}
			if err := s.pipelineInfo.Unmarshal(buf.Bytes()); err != nil {
				return fmt.Errorf("could not unmarshal PipelineInfo bytes from PFS: %v", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("sidecar s3 gateway: could not read pipeline spec commit: %v", err)
		}
		pipelineName := s.pipelineInfo.Pipeline.Name

		// Set auth token for s.pachClient
		resp, err := a.env.GetEtcdClient().Get(retryCtx,
			path.Join(a.env.PPSEtcdPrefix, "pipelines", pipelineName))
		if err != nil {
			return fmt.Errorf("could not get auth token from etcdPipelineInfo: %v", err)
		}
		if len(resp.Kvs) != 1 {
			return fmt.Errorf("expected to find 1 pipeline (%s), got %d: %v",
				pipelineName, len(resp.Kvs), resp)
		}
		var pipelinePtr pps.EtcdPipelineInfo
		if err := pipelinePtr.Unmarshal(resp.Kvs[0].Value); err != nil {
			return fmt.Errorf("sidecar s3 gateway: could not unmarshal etcd pipeline info: %v", err)
		}
		s.pachClient.SetAuthToken(pipelinePtr.AuthToken)
		return nil
	}, backoff.New10sBackOff()); err != nil {
		return fmt.Errorf("error starting sidecar s3 gateway: %v", err)
	}
	if !ppsutil.ContainsS3Inputs(s.pipelineInfo.Input) && !s.pipelineInfo.S3Out {
		return nil // nothing to serve via S3 gateway
	}

	return backoff.RetryNotify(func() (retErr error) {
		return s.Serve()
	}, backoff.NewInfiniteBackOff(), func(err error, d time.Duration) error {
		logrus.Errorf("sidecar s3 gateway: watch closed or error monitoring jobs: %v; retrying in %v", err, d)
		return nil
	})
}

func (s *sidecarS3G) jobInputToBuckets(input *pps.Input) []*s3.Bucket {
	var buckets []*s3.Bucket
	// inputToBuckets is a recursive helper
	pps.VisitInput(input, func(in *pps.Input) {
		if in.Pfs != nil && in.Pfs.S3 {
			name := in.Pfs.Name
			if name == "" {
				name = in.Pfs.Repo
			}
			buckets = append(buckets, &s3.Bucket{
				Repo:   in.Pfs.Repo,
				Commit: in.Pfs.Commit,
				Name:   name,
			})
		}
	})
	return buckets
}

func (s *sidecarS3G) killServer(jobID string) error {
	s.serversMu.Lock()
	defer s.serversMu.Unlock()
	// kill server
	server, ok := s.servers[jobID]
	if !ok {
		// Note that because we call InspectJob after receiving a creation event, we
		// may never create a service for a job that is created and then immediately
		// deleted, so no error is returned if jobID is not in s.servers
		return nil
	}
	if err := server.Close(); err != nil {
		return fmt.Errorf("could not kill sidecar s3 gateway server for job %q: %v", jobID, err)
	}
	delete(s.servers, jobID)
	return nil
}

func (s *sidecarS3G) Serve() error {
	// Watch for new jobs & initialize s3g for each new job
	retryCtx, retryCancel := context.WithCancel(context.Background())
	defer retryCancel()
	watcher, err := s.apiServer.jobs.ReadOnly(retryCtx).WatchByIndex(ppsdb.JobsPipelineIndex, s.pipelineInfo.Pipeline)
	if err != nil {
		return fmt.Errorf("error creating watch: %v", err)
	}
	defer watcher.Close()
	for e := range watcher.Watch() {
		jobID := string(e.Key)
		if e.Type == watch.EventError {
			return fmt.Errorf("sidecar s3 gateway watch error: %v", e.Err)
		} else if e.Type == watch.EventDelete {
			// Job was deleted, e.g. because input commit was deleted. Note that the
			// service may never have been created (see IsErrNotFound under InspectJob
			// below), so no error is returned if jobID is not in s.servers
			s.killServer(jobID)
			continue
		}
		// 'e' is a Put event (new or updated job)
		// create new ctx for this job, and don't use retryCtx as the
		// parent. Just because another job's etcd write failed doesn't
		// mean this job shouldn't run
		jobCtx, jobCancel := context.WithCancel(s.pachClient.Ctx())
		defer jobCancel() // cancel the job ctx
		pachClient := s.pachClient.WithCtx(jobCtx)
		// Inspect the job and make sure it's relevant, as this worker may be old
		logrus.Errorf("sidecar s3 gateway: inspecting job %q to begin serving inputs over s3 gateway", jobID)
		jobInfo, err := pachClient.InspectJob(jobID, false)
		if err != nil {
			if col.IsErrNotFound(err) {
				// TODO(msteffen): I'm not sure what this means--maybe that the service
				// was created and immediately deleted, and there's a pending deletion
				// event? In any case, without input commit IDs there's nothing to do
				return nil
			}
			return fmt.Errorf("error from InspectJob(%v): %+v", jobID, err)
		}
		if jobInfo.PipelineVersion < s.pipelineInfo.Version {
			logrus.Infof("skipping job %v as it uses old pipeline version %d", jobID, jobInfo.PipelineVersion)
			continue
		}
		if jobInfo.PipelineVersion > s.pipelineInfo.Version {
			return fmt.Errorf("job %s's version (%d) greater than pipeline's "+
				"version (%d), this should automatically resolve when the worker "+
				"is updated", jobID, jobInfo.PipelineVersion, s.pipelineInfo.Version)
		}
		if ppsutil.IsTerminal(jobInfo.State) {
			s.killServer(jobID) // previously-created job has finished or been killed
			continue
		}

		// Initialize new S3 gateway
		var outputBucket *s3.Bucket
		if s.pipelineInfo.S3Out == true {
			outputBucket = &s3.Bucket{
				Repo:   jobInfo.OutputCommit.Repo.Name,
				Commit: jobInfo.OutputCommit.ID,
				Name:   "out",
			}
		}
		driver := s3.NewWorkerDriver(s.jobInputToBuckets(jobInfo.Input), outputBucket)

		// server new S3 gateway & add to s.servers
		s.serversMu.Lock()
		defer s.serversMu.Unlock()
		// TODO(msteffen) always serve on the same port for now (there shouldn't be
		// more than one job in s.servers). When parallel jobs are implemented, the
		// servers in s.servers won't actually serve anymore, and instead parent
		// server will forward requests based on the request hostname
		port := s.apiServer.env.S3GatewayPort
		server, err := s3.Server(port, driver, s)
		if err != nil {
			return fmt.Errorf("sidecar s3 gateway: couldn't initialize s3 gateway server: %v", err)
		}
		strport := strconv.FormatInt(int64(port), 10)
		listener, err := net.Listen("tcp", ":"+strport)
		if err != nil {
			return fmt.Errorf("sidecar s3 gateway: could not serve on port %q: %v", port, err)
		}
		go func() {
			server.Serve(listener)
		}()
		s.servers[jobID] = server
	}
	return fmt.Errorf("sidecar s3 gateway: Serve() is exiting, which shouldn't happen")
}

func (s *sidecarS3G) Client(authToken string) (*client.APIClient, error) {
	newClient := s.apiServer.env.GetPachClient(s.pachClient.Ctx()) // clones s.pachClient
	newClient.SetAuthToken(authToken)
	return newClient, nil
}
