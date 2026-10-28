package runtimecodec

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	pb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ChapterFormatVersion = 2

func executionValue(raw []byte) (*pb.ExecutionValue, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return valueToExecution(v)
}
func valueToExecution(v any) (*pb.ExecutionValue, error) {
	p := new(pb.ExecutionValue)
	switch v := v.(type) {
	case nil:
		p.SetNullValue(true)
	case bool:
		p.SetBoolValue(v)
	case string:
		p.SetStringValue(v)
	case json.Number:
		p.SetNumberValue(v.String())
	case []any:
		a := new(pb.ExecutionArray)
		items := make([]*pb.ExecutionValue, 0, len(v))
		for _, x := range v {
			y, e := valueToExecution(x)
			if e != nil {
				return nil, e
			}
			items = append(items, y)
		}
		a.SetItems(items)
		p.SetArrayValue(a)
	case map[string]any:
		o := new(pb.ExecutionObject)
		fields := map[string]*pb.ExecutionValue{}
		for k, x := range v {
			y, e := valueToExecution(x)
			if e != nil {
				return nil, e
			}
			fields[k] = y
		}
		o.SetFields(fields)
		p.SetObjectValue(o)
	default:
		return nil, fmt.Errorf("unsupported execution value %T", v)
	}
	return p, nil
}
func executionToValue(p *pb.ExecutionValue) any {
	if p == nil {
		return nil
	}
	switch {
	case p.HasNullValue():
		return nil
	case p.HasBoolValue():
		return p.GetBoolValue()
	case p.HasStringValue():
		return p.GetStringValue()
	case p.HasNumberValue():
		return json.Number(p.GetNumberValue())
	case p.HasArrayValue():
		a := make([]any, 0, len(p.GetArrayValue().GetItems()))
		for _, v := range p.GetArrayValue().GetItems() {
			a = append(a, executionToValue(v))
		}
		return a
	case p.HasObjectValue():
		m := map[string]any{}
		for k, v := range p.GetObjectValue().GetFields() {
			m[k] = executionToValue(v)
		}
		return m
	}
	return nil
}
func executionJSON(p *pb.ExecutionValue) (json.RawMessage, error) {
	if p == nil {
		return nil, nil
	}
	return json.Marshal(executionToValue(p))
}
func executionTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
func snapshotToProto(s *jobdb.ExecutionSnapshot) *pb.ExecutionSnapshot {
	if s == nil {
		return nil
	}
	p := new(pb.ExecutionSnapshot)
	p.SetData(s.Data)
	arts := make([]*pb.ExecutionArtifact, 0, len(s.Artifacts))
	for _, a := range s.Artifacts {
		k := a.Key
		v := new(pb.ExecutionArtifact)
		v.SetName(a.Name)
		v.SetJobId(k.JobId)
		v.SetOrdinal(k.TaskOrdinal)
		v.SetStoredName(k.Name)
		v.SetSizeBytes(k.SizeBytes)
		arts = append(arts, v)
	}
	p.SetArtifacts(arts)
	return p
}
func snapshotFromProto(p *pb.ExecutionSnapshot) *jobdb.ExecutionSnapshot {
	if p == nil {
		return nil
	}
	s := &jobdb.ExecutionSnapshot{Data: cloneJSON(p.GetData())}
	for _, a := range p.GetArtifacts() {
		s.Artifacts = append(s.Artifacts, jobdb.ExecutionArtifact{Name: a.GetName(), Key: &jobdb.ArtifactKey{JobId: a.GetJobId(), TaskOrdinal: a.GetOrdinal(), Name: a.GetStoredName(), SizeBytes: a.GetSizeBytes()}})
	}
	return s
}
func ExecutionToProto(r jobdb.ExecutionRecord) (*pb.ExecutionRecord, error) {
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if _, err = jobdb.DecodeExecutionRecord(raw); err != nil {
		return nil, err
	}
	p := new(pb.ExecutionRecord)
	p.SetVersion(r.Version)
	p.SetKind(r.Kind)
	p.SetId(r.ID)
	p.SetActivation(r.Activation)
	if r.Versions != nil {
		v := new(pb.ExecutionVersions)
		v.SetExecutionPatternVersion(r.Versions.ExecutionPatternVersion)
		v.SetJobdbVersion(r.Versions.JobDBVersion)
		v.SetConsumerVersions(r.Versions.ConsumerVersions)
		p.SetVersions(v)
	}
	p.SetAt(executionTime(r.At))
	p.SetAttempt(int32(r.Attempt))
	p.SetDeadline(executionTime(r.Deadline))
	p.SetTotalDeadline(executionTime(r.TotalDeadline))
	p.SetInput(snapshotToProto(r.Input))
	p.SetOutput(snapshotToProto(r.Output))
	p.SetRule(r.Rule)
	p.SetDisposition(r.Disposition)
	if r.Retryable != nil {
		p.SetRetryable(*r.Retryable)
	}
	p.SetRetry(r.Retry)
	p.SetWakeAt(executionTime(r.WakeAt))
	if r.Source != nil {
		p.SetSourceTenant(r.Source.TenantId)
		p.SetSourceJob(r.Source.JobId)
	}
	if r.AlternateAt != nil {
		p.SetAlternateAt(executionTime(*r.AlternateAt))
	}
	p.SetExecutionTaskType(r.ExecutionTaskType)
	p.SetPrerequisites(prerequisitesToProto(r.Prerequisites))
	if len(r.Dependencies) > 0 {
		raw, _ := json.Marshal(r.Dependencies)
		v, e := executionValue(raw)
		if e != nil {
			return nil, e
		}
		p.SetDependencies(v)
	}
	if r.Kind == "restart" && len(r.Descriptor) > 0 {
		rs, e := executionRecordsToProto(r.Descriptor)
		if e != nil {
			return nil, e
		}
		p.SetRetained(rs)
	} else {
		v, e := executionValue(r.Descriptor)
		if e != nil {
			return nil, e
		}
		p.SetDescriptor(v)
	}
	switch r.Kind {
	case "continuation", "dependency_binding":
		v, e := executionValue(r.Payload)
		if e != nil {
			return nil, e
		}
		p.SetLifecyclePayload(v)
	default:
		p.SetApplicationPayload(r.Payload)
	}
	if r.ErrorKind != "" {
		v, e := taskOutcomeToProto(r.ErrorKind, r.Error)
		if e != nil {
			return nil, e
		}
		p.SetError(v)
	}
	return p, nil
}
func executionFromProto(p *pb.ExecutionRecord) (jobdb.ExecutionRecord, error) {
	if p == nil {
		return jobdb.ExecutionRecord{}, fmt.Errorf("execution record required")
	}
	r := jobdb.ExecutionRecord{Version: p.GetVersion(), Kind: p.GetKind(), ID: p.GetId(), Activation: p.GetActivation(), At: timestampToTime(p.GetAt()), Attempt: int(p.GetAttempt()), Deadline: timestampToTime(p.GetDeadline()), TotalDeadline: timestampToTime(p.GetTotalDeadline()), Input: snapshotFromProto(p.GetInput()), Output: snapshotFromProto(p.GetOutput()), Rule: p.GetRule(), Disposition: p.GetDisposition(), Retry: p.GetRetry(), WakeAt: timestampToTime(p.GetWakeAt()), ExecutionTaskType: p.GetExecutionTaskType(), Payload: cloneJSON(p.GetApplicationPayload()), Prerequisites: prerequisitesFromProto(p.GetPrerequisites())}
	if p.HasVersions() {
		v := p.GetVersions()
		r.Versions = &jobdb.ExecutionVersions{ExecutionPatternVersion: v.GetExecutionPatternVersion(), JobDBVersion: v.GetJobdbVersion(), ConsumerVersions: v.GetConsumerVersions()}
	}
	if p.HasRetryable() {
		b := p.GetRetryable()
		r.Retryable = &b
	}
	if p.HasAlternateAt() {
		t := timestampToTime(p.GetAlternateAt())
		r.AlternateAt = &t
	}
	if p.HasSourceJob() {
		r.Source = &jobdb.JobKey{TenantId: p.GetSourceTenant(), JobId: p.GetSourceJob()}
	}
	var err error
	if p.HasDependencies() {
		raw, e := executionJSON(p.GetDependencies())
		if e != nil {
			return r, e
		}
		if e = json.Unmarshal(raw, &r.Dependencies); e != nil {
			return r, e
		}
	}
	if r.Kind == "restart" {
		r.Descriptor, err = executionRecordsJSON(p.GetRetained())
	} else {
		r.Descriptor, err = executionJSON(p.GetDescriptor())
	}
	if err != nil {
		return r, err
	}
	if p.HasLifecyclePayload() {
		r.Payload, err = executionJSON(p.GetLifecyclePayload())
		if err != nil {
			return r, err
		}
	}
	if p.HasError() {
		r.ErrorKind, r.Error, err = taskOutcomeFromProto(p.GetError())
		if err != nil {
			return r, err
		}
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	if _, err = jobdb.DecodeExecutionRecord(raw); err != nil {
		return r, err
	}
	return r, nil
}
func executionRecordsToProto(raw []byte) ([]*pb.ExecutionRecord, error) {
	var records []json.RawMessage
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, err
	}
	out := make([]*pb.ExecutionRecord, 0, len(records))
	for _, raw := range records {
		r, e := jobdb.DecodeExecutionRecord(raw)
		if e != nil {
			return nil, e
		}
		p, e := ExecutionToProto(r)
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, nil
}
func executionRecordsJSON(records []*pb.ExecutionRecord) (json.RawMessage, error) {
	if len(records) == 0 {
		return nil, nil
	}
	out := make([]jobdb.ExecutionRecord, 0, len(records))
	for _, p := range records {
		r, e := executionFromProto(p)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return json.Marshal(out)
}
func extractReconstruction(meta *ChapterMeta) ([]*pb.ExecutionRecord, error) {
	if len(meta.Metadata) == 0 {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(meta.Metadata, &fields); err != nil {
		return nil, err
	}
	raw, ok := fields[executionmeta.Key]
	if !ok {
		return nil, nil
	}
	var str string
	if err := json.Unmarshal(raw, &str); err != nil {
		return nil, err
	}
	records, err := executionRecordsToProto([]byte(str))
	if err != nil {
		return nil, err
	}
	delete(fields, executionmeta.Key)
	meta.Metadata, err = json.Marshal(fields)
	return records, err
}
func restoreReconstruction(meta *ChapterMeta, records []*pb.ExecutionRecord) error {
	if len(records) == 0 {
		return nil
	}
	raw, err := executionRecordsJSON(records)
	if err != nil {
		return err
	}
	fields := map[string]json.RawMessage{}
	if len(meta.Metadata) > 0 {
		if err = json.Unmarshal(meta.Metadata, &fields); err != nil {
			return err
		}
	}
	fields[executionmeta.Key], err = json.Marshal(string(raw))
	if err != nil {
		return err
	}
	meta.Metadata, err = json.Marshal(fields)
	return err
}

// ExecutionIdentity is the version-4 deterministic storage identity, shared by
// wait producers, consumers, and migration. It never hashes the public JSON view.
func ExecutionIdentity(r jobdb.ExecutionRecord) (string, error) {
	// The public execution JSON boundary compacts embedded RawMessage values.
	// Normalize that presentation once so a producer and its persisted record
	// identify the same wait, even when the caller supplied JSON whitespace.
	raw, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	r, err = jobdb.DecodeExecutionRecord(raw)
	if err != nil {
		return "", err
	}
	p, err := ExecutionToProto(r)
	if err != nil {
		return "", err
	}
	raw, err = deterministicMarshal.Marshal(p)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}
func rejectUnknown(m proto.Message) error { return rejectUnknownMessage(m.ProtoReflect(), 0) }
func rejectUnknownMessage(m protoreflect.Message, depth int) error {
	if depth > 128 {
		return fmt.Errorf("protobuf nesting exceeds 128")
	}
	if value, ok := m.Interface().(interface{ CheckValid() error }); ok {
		if err := value.CheckValid(); err != nil {
			return fmt.Errorf("invalid %s: %w", m.Descriptor().FullName(), err)
		}
	}
	if len(m.GetUnknown()) > 0 {
		return fmt.Errorf("unsupported protobuf fields in %s", m.Descriptor().FullName())
	}
	var err error
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.IsMap() {
			if f.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, x protoreflect.Value) bool {
					err = rejectUnknownMessage(x.Message(), depth+1)
					return err == nil
				})
			}
		} else if f.IsList() {
			if f.Kind() == protoreflect.MessageKind {
				list := v.List()
				for i := 0; i < list.Len() && err == nil; i++ {
					err = rejectUnknownMessage(list.Get(i).Message(), depth+1)
				}
			}
		} else if f.Kind() == protoreflect.MessageKind {
			err = rejectUnknownMessage(v.Message(), depth+1)
		}
		return err == nil
	})
	return err
}
