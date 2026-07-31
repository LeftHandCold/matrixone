// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const frozenArchiveTargetVersion uint16 = 1

// FrozenArchiveTarget is persisted in Dataset and Cleanup Root. It contains no
// secret material: CredentialHandle resolves through deployment credentials.
type FrozenArchiveTarget struct {
	FormatVersion      uint16 `json:"format_version"`
	StageID            uint64 `json:"stage_id"`
	Provider           string `json:"provider"`
	CanonicalEndpoint  string `json:"canonical_endpoint"`
	Region             string `json:"region"`
	BucketOrContainer  string `json:"bucket_or_container"`
	ImmutablePrefix    string `json:"immutable_prefix"`
	StorageClass       string `json:"storage_class,omitempty"`
	EncryptionIdentity string `json:"encryption_identity,omitempty"`
	CredentialHandle   string `json:"credential_handle"`
}

func ParseFrozenArchiveTarget(encoded []byte) (FrozenArchiveTarget, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var target FrozenArchiveTarget
	if err := decoder.Decode(&target); err != nil {
		return FrozenArchiveTarget{}, err
	}
	if err := target.Validate(); err != nil {
		return FrozenArchiveTarget{}, err
	}
	return target, nil
}

func (target FrozenArchiveTarget) Marshal() ([]byte, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(target)
}

func (target FrozenArchiveTarget) Validate() error {
	if target.FormatVersion != frozenArchiveTargetVersion ||
		target.StageID == 0 ||
		target.Provider == "" ||
		target.Region == "" ||
		target.BucketOrContainer == "" ||
		target.CredentialHandle == "" {
		return fmt.Errorf("Lifecycle frozen Archive target is incomplete")
	}
	if err := ValidateArchiveCredentialHandle(target.CredentialHandle); err != nil {
		return err
	}
	if strings.Contains(target.ImmutablePrefix, "..") {
		return fmt.Errorf("Lifecycle frozen Archive prefix is invalid")
	}
	if target.StorageClass != "" &&
		!strings.EqualFold(target.StorageClass, "STANDARD") {
		return fmt.Errorf(
			"Lifecycle Phase 1 supports only the STANDARD storage class",
		)
	}
	return nil
}

// ValidateArchiveCredentialHandle keeps SET LIFECYCLE admission aligned with
// the credential mechanisms that NewArchiveFileService can actually resolve.
// Supporting a new deployment alias requires implementing its resolver before
// it can be advertised in a release allowlist.
func ValidateArchiveCredentialHandle(handle string) error {
	switch {
	case handle == "default":
		return nil
	case strings.HasPrefix(handle, "role-arn:"):
		if strings.TrimPrefix(handle, "role-arn:") != "" {
			return nil
		}
	case strings.HasPrefix(handle, "shared-profile:"):
		if strings.TrimPrefix(handle, "shared-profile:") != "" {
			return nil
		}
	}
	return fmt.Errorf(
		"unsupported Lifecycle deployment credential handle %q",
		handle,
	)
}

// NewArchiveFileService resolves only deployment-managed credential handles.
// Inline Stage keys are deliberately not accepted.
func NewArchiveFileService(
	ctx context.Context,
	target FrozenArchiveTarget,
) (fileservice.FileService, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	arguments := fileservice.ObjectStorageArguments{
		Name:      fmt.Sprintf("lifecycle-stage-%d", target.StageID),
		KeyPrefix: target.ImmutablePrefix,
		Bucket:    target.BucketOrContainer,
		Endpoint:  target.CanonicalEndpoint,
		Region:    target.Region,
		IsMinio:   strings.EqualFold(target.Provider, "minio"),
	}
	switch {
	case target.CredentialHandle == "default":
		// Workload identity / instance role through the existing provider chain.
	case strings.HasPrefix(target.CredentialHandle, "role-arn:"):
		arguments.RoleARN = strings.TrimPrefix(target.CredentialHandle, "role-arn:")
		if arguments.RoleARN == "" {
			return nil, fmt.Errorf("Lifecycle Archive role ARN is empty")
		}
	case strings.HasPrefix(target.CredentialHandle, "shared-profile:"):
		arguments.SharedConfigProfile = strings.TrimPrefix(
			target.CredentialHandle,
			"shared-profile:",
		)
	}
	return fileservice.NewS3FS(
		ctx,
		arguments,
		fileservice.CacheConfig{},
		nil,
		true,
		false,
	)
}

// FileServiceArchiveStore adapts the existing write-once FileService contract
// to Lifecycle Archive and cleanup. It does not add a second provider client.
type FileServiceArchiveStore struct {
	FileService    fileservice.FileService
	MaxListEntries int
}

func (store FileServiceArchiveStore) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	if store.FileService == nil {
		return fmt.Errorf("Lifecycle Archive FileService is nil")
	}
	err := store.FileService.Write(ctx, fileservice.IOVector{
		FilePath: key,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(value)),
			Data:   value,
		}},
	})
	if !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		return err
	}
	existing, readErr := store.Get(ctx, key)
	if readErr != nil {
		return readErr
	}
	if !bytes.Equal(existing, value) {
		return fmt.Errorf("Lifecycle immutable Archive key %s already has different content", key)
	}
	return nil
}

func (store FileServiceArchiveStore) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	if store.FileService == nil {
		return nil, fmt.Errorf("Lifecycle Archive FileService is nil")
	}
	vector := fileservice.IOVector{
		FilePath: key,
		Policy:   fileservice.SkipAllCache,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := store.FileService.Read(ctx, &vector); err != nil {
		vector.ReleaseReadResultOnError()
		return nil, err
	}
	defer vector.Release()
	return append([]byte(nil), vector.Entries[0].Data...), nil
}

func (store FileServiceArchiveStore) List(
	ctx context.Context,
	prefix string,
) ([]string, error) {
	if store.FileService == nil {
		return nil, fmt.Errorf("Lifecycle Archive FileService is nil")
	}
	limit := store.MaxListEntries
	if limit <= 0 {
		limit = 100_000
	}
	directories := []string{strings.TrimSuffix(prefix, "/")}
	keys := make([]string, 0)
	for len(directories) > 0 {
		directory := directories[len(directories)-1]
		directories = directories[:len(directories)-1]
		for entry, err := range store.FileService.List(ctx, directory) {
			if err != nil {
				return nil, err
			}
			fullPath := path.Join(directory, entry.Name)
			if entry.IsDir {
				directories = append(directories, fullPath)
			} else {
				keys = append(keys, fullPath)
			}
			if len(keys)+len(directories) > limit {
				return nil, fmt.Errorf("Lifecycle cleanup prefix exceeds list limit %d", limit)
			}
		}
	}
	return keys, nil
}

func (store FileServiceArchiveStore) Delete(
	ctx context.Context,
	key string,
) error {
	if store.FileService == nil {
		return fmt.Errorf("Lifecycle Archive FileService is nil")
	}
	err := store.FileService.Delete(ctx, key)
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return nil
	}
	return err
}
