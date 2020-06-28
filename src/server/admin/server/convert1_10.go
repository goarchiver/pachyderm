package server

import (
	"github.com/pachyderm/pachyderm/src/client/admin"
	pfs1_10 "github.com/pachyderm/pachyderm/src/client/admin/v1_10/pfs"
	pps1_10 "github.com/pachyderm/pachyderm/src/client/admin/v1_10/pps"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pps"
)

func convert1_10Repo(r *pfs1_10.Repo) *pfs.Repo {
	if r == nil {
		return nil
	}
	return &pfs.Repo{
		Name: r.Name,
	}
}

func convert1_10Commit(c *pfs1_10.Commit) *pfs.Commit {
	if c == nil {
		return nil
	}
	return &pfs.Commit{
		Repo: convert1_10Repo(c.Repo),
		ID:   c.ID,
	}
}

func convert1_10Provenance(provenance *pfs1_10.CommitProvenance) *pfs.CommitProvenance {
	if provenance == nil {
		return nil
	}
	return &pfs.CommitProvenance{
		Commit: convert1_10Commit(provenance.Commit),
		Branch: convert1_10Branch(provenance.Branch),
	}
}

func convert1_10Provenances(provenances []*pfs1_10.CommitProvenance) []*pfs.CommitProvenance {
	if provenances == nil {
		return nil
	}
	result := make([]*pfs.CommitProvenance, 0, len(provenances))
	for _, p := range provenances {
		result = append(result, convert1_10Provenance(p))
	}
	return result
}

func convert1_10Job(j *pps1_10.CreateJobRequest) *pps.CreateJobRequest {
	if j == nil {
		return nil
	}
	return &pps.CreateJobRequest{
		Pipeline:      convert1_10Pipeline(j.Pipeline),
		OutputCommit:  convert1_10Commit(j.OutputCommit),
		Restart:       j.Restart,
		DataProcessed: j.DataProcessed,
		DataSkipped:   j.DataSkipped,
		DataTotal:     j.DataTotal,
		DataFailed:    j.DataFailed,
		DataRecovered: j.DataRecovered,
		Stats:         convert1_10Stats(j.Stats),
		StatsCommit:   convert1_10Commit(j.StatsCommit),
		State:         pps.JobState(j.State),
		Reason:        j.Reason,
		Started:       j.Started,
		Finished:      j.Finished,
	}
}

func convert1_10Stats(s *pps1_10.ProcessStats) *pps.ProcessStats {
	if s == nil {
		return nil
	}
	return &pps.ProcessStats{
		DownloadTime:  s.DownloadTime,
		ProcessTime:   s.ProcessTime,
		UploadTime:    s.UploadTime,
		DownloadBytes: s.DownloadBytes,
		UploadBytes:   s.UploadBytes,
	}
}

func convert1_10CreateObject(o *pfs1_10.CreateObjectRequest) *pfs.CreateObjectRequest {
	if o == nil {
		return nil
	}
	return &pfs.CreateObjectRequest{
		Object:   convert1_10Object(o.Object),
		BlockRef: convert1_10BlockRef(o.BlockRef),
	}
}

func convert1_10Object(o *pfs1_10.Object) *pfs.Object {
	if o == nil {
		return nil
	}
	return &pfs.Object{
		Hash: o.Hash,
	}
}

func convert1_10BlockRef(b *pfs1_10.BlockRef) *pfs.BlockRef {
	if b == nil {
		return nil
	}
	return &pfs.BlockRef{
		Block: &pfs.Block{
			Hash: b.Block.Hash,
		},
		Range: &pfs.ByteRange{
			Lower: b.Range.Lower,
			Upper: b.Range.Upper,
		},
	}
}

func convert1_10Objects(objects []*pfs1_10.Object) []*pfs.Object {
	if objects == nil {
		return nil
	}
	result := make([]*pfs.Object, 0, len(objects))
	for _, o := range objects {
		result = append(result, convert1_10Object(o))
	}
	return result
}

func convert1_10Tag(tag *pfs1_10.Tag) *pfs.Tag {
	if tag == nil {
		return nil
	}
	return &pfs.Tag{
		Name: tag.Name,
	}
}

func convert1_10Tags(tags []*pfs1_10.Tag) []*pfs.Tag {
	if tags == nil {
		return nil
	}
	result := make([]*pfs.Tag, 0, len(tags))
	for _, t := range tags {
		result = append(result, convert1_10Tag(t))
	}
	return result
}

func convert1_10Branch(b *pfs1_10.Branch) *pfs.Branch {
	if b == nil {
		return nil
	}
	return &pfs.Branch{
		Repo: convert1_10Repo(b.Repo),
		Name: b.Name,
	}
}

func convert1_10Branches(branches []*pfs1_10.Branch) []*pfs.Branch {
	if branches == nil {
		return nil
	}
	result := make([]*pfs.Branch, 0, len(branches))
	for _, b := range branches {
		result = append(result, convert1_10Branch(b))
	}
	return result
}

func convert1_10Pipeline(p *pps1_10.Pipeline) *pps.Pipeline {
	if p == nil {
		return nil
	}
	return &pps.Pipeline{
		Name: p.Name,
	}
}

func convert1_10SecretMount(s *pps1_10.SecretMount) *pps.SecretMount {
	if s == nil {
		return nil
	}
	return &pps.SecretMount{
		Name:      s.Name,
		Key:       s.Key,
		MountPath: s.MountPath,
		EnvVar:    s.EnvVar,
	}
}

func convert1_10SecretMounts(secrets []*pps1_10.SecretMount) []*pps.SecretMount {
	if secrets == nil {
		return nil
	}
	result := make([]*pps.SecretMount, 0, len(secrets))
	for _, s := range secrets {
		result = append(result, convert1_10SecretMount(s))
	}
	return result
}

func convert1_10Transform(t *pps1_10.Transform) *pps.Transform {
	if t == nil {
		return nil
	}
	return &pps.Transform{
		Image:            t.Image,
		Cmd:              t.Cmd,
		ErrCmd:           t.ErrCmd,
		Env:              t.Env,
		Secrets:          convert1_10SecretMounts(t.Secrets),
		ImagePullSecrets: t.ImagePullSecrets,
		Stdin:            t.Stdin,
		ErrStdin:         t.ErrStdin,
		AcceptReturnCode: t.AcceptReturnCode,
		Debug:            t.Debug,
		User:             t.User,
		WorkingDir:       t.WorkingDir,
		Dockerfile:       t.Dockerfile,
	}
}

func convert1_10ParallelismSpec(s *pps1_10.ParallelismSpec) *pps.ParallelismSpec {
	if s == nil {
		return nil
	}
	return &pps.ParallelismSpec{
		Constant:    s.Constant,
		Coefficient: s.Coefficient,
	}
}

func convert1_10HashtreeSpec(h *pps1_10.HashtreeSpec) *pps.HashtreeSpec {
	if h == nil {
		return nil
	}
	return &pps.HashtreeSpec{
		Constant: h.Constant,
	}
}

func convert1_10Egress(e *pps1_10.Egress) *pps.Egress {
	if e == nil {
		return nil
	}
	return &pps.Egress{
		URL: e.URL,
	}
}

func convert1_10GPUSpec(g *pps1_10.GPUSpec) *pps.GPUSpec {
	if g == nil {
		return nil
	}
	return &pps.GPUSpec{
		Type:   g.Type,
		Number: g.Number,
	}
}

func convert1_10ResourceSpec(r *pps1_10.ResourceSpec) *pps.ResourceSpec {
	if r == nil {
		return nil
	}
	return &pps.ResourceSpec{
		Cpu:    r.Cpu,
		Memory: r.Memory,
		Gpu:    convert1_10GPUSpec(r.Gpu),
		Disk:   r.Disk,
	}
}

func convert1_10PFSInput(p *pps1_10.PFSInput) *pps.PFSInput {
	if p == nil {
		return nil
	}
	return &pps.PFSInput{
		Name:       p.Name,
		Repo:       p.Repo,
		Branch:     p.Branch,
		Commit:     p.Commit,
		Glob:       p.Glob,
		JoinOn:     p.JoinOn,
		Lazy:       p.Lazy,
		EmptyFiles: p.EmptyFiles,
	}
}

func convert1_10CronInput(i *pps1_10.CronInput) *pps.CronInput {
	if i == nil {
		return nil
	}
	return &pps.CronInput{
		Name:      i.Name,
		Repo:      i.Repo,
		Commit:    i.Commit,
		Spec:      i.Spec,
		Overwrite: i.Overwrite,
		Start:     i.Start,
	}
}

func convert1_10GitInput(i *pps1_10.GitInput) *pps.GitInput {
	if i == nil {
		return nil
	}
	return &pps.GitInput{
		Name:   i.Name,
		URL:    i.URL,
		Branch: i.Branch,
		Commit: i.Commit,
	}
}

func convert1_10Input(i *pps1_10.Input) *pps.Input {
	if i == nil {
		return nil
	}
	return &pps.Input{
		Pfs:   convert1_10PFSInput(i.Pfs),
		Cross: convert1_10Inputs(i.Cross),
		Union: convert1_10Inputs(i.Union),
		Cron:  convert1_10CronInput(i.Cron),
		Git:   convert1_10GitInput(i.Git),
	}
}

func convert1_10Inputs(inputs []*pps1_10.Input) []*pps.Input {
	if inputs == nil {
		return nil
	}
	result := make([]*pps.Input, 0, len(inputs))
	for _, i := range inputs {
		result = append(result, convert1_10Input(i))
	}
	return result
}

func convert1_10Service(s *pps1_10.Service) *pps.Service {
	if s == nil {
		return nil
	}
	return &pps.Service{
		InternalPort: s.InternalPort,
		ExternalPort: s.ExternalPort,
		IP:           s.IP,
		Type:         s.Type,
	}
}

func convert1_10Spout(s *pps1_10.Spout) *pps.Spout {
	if s == nil {
		return nil
	}
	return &pps.Spout{
		Overwrite: s.Overwrite,
		Service:   convert1_10Service(s.Service),
		Marker:    s.Marker,
	}
}

func convert1_10Metadata(s *pps1_10.Metadata) *pps.Metadata {
	if s == nil {
		return nil
	}
	if s.Annotations == nil {
		return nil
	}
	return &pps.Metadata{
		Annotations: s.Annotations,
	}
}

func convert1_10ChunkSpec(c *pps1_10.ChunkSpec) *pps.ChunkSpec {
	if c == nil {
		return nil
	}
	return &pps.ChunkSpec{
		Number:    c.Number,
		SizeBytes: c.SizeBytes,
	}
}

func convert1_10SchedulingSpec(s *pps1_10.SchedulingSpec) *pps.SchedulingSpec {
	if s == nil {
		return nil
	}
	return &pps.SchedulingSpec{
		NodeSelector:      s.NodeSelector,
		PriorityClassName: s.PriorityClassName,
	}
}

func convert1_10Op(op *admin.Op1_10) (*admin.Op1_11, error) {
	switch {
	case op.CreateObject != nil:
		return &admin.Op1_11{
			CreateObject: convert1_10CreateObject(op.CreateObject),
		}, nil
	case op.Job != nil:
		return &admin.Op1_11{
			Job: convert1_10Job(op.Job),
		}, nil
	case op.Tag != nil:
		if !objHashRE.MatchString(op.Tag.Object.Hash) {
			return nil, errors.Errorf("invalid object hash in op: %q", op)
		}
		return &admin.Op1_11{
			Tag: &pfs.TagObjectRequest{
				Object: convert1_10Object(op.Tag.Object),
				Tags:   convert1_10Tags(op.Tag.Tags),
			},
		}, nil
	case op.Repo != nil:
		return &admin.Op1_11{
			Repo: &pfs.CreateRepoRequest{
				Repo:        convert1_10Repo(op.Repo.Repo),
				Description: op.Repo.Description,
			},
		}, nil
	case op.Commit != nil:
		return &admin.Op1_11{
			Commit: &pfs.BuildCommitRequest{
				Parent:     convert1_10Commit(op.Commit.Parent),
				Branch:     op.Commit.Branch,
				Provenance: convert1_10Provenances(op.Commit.Provenance),
				Tree:       convert1_10Object(op.Commit.Tree),
				Trees:      convert1_10Objects(op.Commit.Trees),
				Datums:     convert1_10Object(op.Commit.Datums),
				ID:         op.Commit.ID,
				SizeBytes:  op.Commit.SizeBytes,
			},
		}, nil
	case op.Branch != nil:
		newOp := &admin.Op1_11{
			Branch: &pfs.CreateBranchRequest{
				Head:       convert1_10Commit(op.Branch.Head),
				Branch:     convert1_10Branch(op.Branch.Branch),
				Provenance: convert1_10Branches(op.Branch.Provenance),
			},
		}
		if newOp.Branch.Branch == nil {
			newOp.Branch.Branch = &pfs.Branch{
				Repo: convert1_10Repo(op.Branch.Head.Repo),
				Name: op.Branch.SBranch,
			}
		}
		return newOp, nil
	case op.Pipeline != nil:
		return &admin.Op1_11{
			Pipeline: &pps.CreatePipelineRequest{
				Pipeline:         convert1_10Pipeline(op.Pipeline.Pipeline),
				Transform:        convert1_10Transform(op.Pipeline.Transform),
				ParallelismSpec:  convert1_10ParallelismSpec(op.Pipeline.ParallelismSpec),
				HashtreeSpec:     convert1_10HashtreeSpec(op.Pipeline.HashtreeSpec),
				Egress:           convert1_10Egress(op.Pipeline.Egress),
				Update:           op.Pipeline.Update,
				OutputBranch:     op.Pipeline.OutputBranch,
				ResourceRequests: convert1_10ResourceSpec(op.Pipeline.ResourceRequests),
				ResourceLimits:   convert1_10ResourceSpec(op.Pipeline.ResourceLimits),
				Input:            convert1_10Input(op.Pipeline.Input),
				Description:      op.Pipeline.Description,
				CacheSize:        op.Pipeline.CacheSize,
				EnableStats:      op.Pipeline.EnableStats,
				Reprocess:        op.Pipeline.Reprocess,
				MaxQueueSize:     op.Pipeline.MaxQueueSize,
				Service:          convert1_10Service(op.Pipeline.Service),
				Spout:            convert1_10Spout(op.Pipeline.Spout),
				ChunkSpec:        convert1_10ChunkSpec(op.Pipeline.ChunkSpec),
				DatumTimeout:     op.Pipeline.DatumTimeout,
				JobTimeout:       op.Pipeline.JobTimeout,
				Salt:             op.Pipeline.Salt,
				Standby:          op.Pipeline.Standby,
				DatumTries:       op.Pipeline.DatumTries,
				SchedulingSpec:   convert1_10SchedulingSpec(op.Pipeline.SchedulingSpec),
				PodSpec:          op.Pipeline.PodSpec,
				PodPatch:         op.Pipeline.PodPatch,
				SpecCommit:       convert1_10Commit(op.Pipeline.SpecCommit),
				Metadata:         convert1_10Metadata(op.Pipeline.Metadata),
			},
		}, nil
	default:
		return nil, errors.Errorf("unrecognized 1.9 op type:\n%+v", op)
	}
	return nil, errors.Errorf("internal error: convert1_10Op() didn't return a 1.9 op for:\n%+v", op)
}
