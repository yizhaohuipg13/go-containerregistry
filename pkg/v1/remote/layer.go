// Copyright 2019 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/google/go-containerregistry/internal/redact"
	"github.com/google/go-containerregistry/internal/verify"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// remoteImagelayer implements partial.CompressedLayer
type remoteLayer struct {
	fetcher
	digest v1.Hash
}

// Compressed implements partial.CompressedLayer
func (rl *remoteLayer) Compressed() (io.ReadCloser, error) {
	// We don't want to log binary layers -- this can break terminals.
	ctx := redact.NewContext(rl.context, "omitting binary blobs from logs")
	return rl.fetchBlob(ctx, verify.SizeUnknown, rl.digest)
}

// Compressed implements partial.CompressedLayer
func (rl *remoteLayer) Size() (int64, error) {
	resp, err := rl.headBlob(rl.digest)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	return resp.ContentLength, nil
}

// Digest implements partial.CompressedLayer
func (rl *remoteLayer) Digest() (v1.Hash, error) {
	return rl.digest, nil
}

// MediaType implements v1.Layer
func (rl *remoteLayer) MediaType() (types.MediaType, error) {
	return types.DockerLayer, nil
}

// See partial.Exists.
func (rl *remoteLayer) Exists() (bool, error) {
	return rl.blobExists(rl.digest)
}

func downloadLayer(ref name.Digest, options ...Option) (v1.Layer, v1.Hash, error) {
	o, err := makeOptions(ref.Context(), options...)
	if err != nil {
		return nil, v1.Hash{}, err
	}
	f, err := makeFetcher(ref, o)
	if err != nil {
		return nil, v1.Hash{}, err
	}
	h, err := v1.NewHash(ref.Identifier())
	if err != nil {
		return nil, v1.Hash{}, err
	}
	l, err := partial.CompressedToLayer(&remoteLayer{
		fetcher: *f,
		digest:  h,
	})
	if err != nil {
		return nil, v1.Hash{}, err
	}
	return l, h, nil
}

// Layer reads the given blob reference from a registry as a Layer. A blob
// reference here is just a punned name.Digest where the digest portion is the
// digest of the blob to be read and the repository portion is the repo where
// that blob lives.
func Layer(ref name.Digest, options ...Option) (v1.Layer, error) {
	l, _, err := downloadLayer(ref, options...)
	if err != nil {
		return nil, err
	}
	return &MountableLayer{
		Layer:     l,
		Reference: ref,
	}, nil
}

// SingleLayer reads the given blob reference from a registry as a Layer. A blob
// reference here is just a punned name.Digest where the digest portion is the
// digest to the blob to be read and the repository portion is the repo where
// that blob lives. Difference with Layer, SingleLayer return v1.Layer data without Reference.
func SingleLayer(ref name.Digest, options ...Option) (v1.Layer, v1.Hash, error) {
	return downloadLayer(ref, options...)
}

// SaveSpecifyLayers reads the given blob reference from a registry as a Layer.
// Skip the specified layer, the digest of the layer used by the parameter refs.
// The absolute path used by the parameter path, including the tar name.
// options should contain remote.Option.
func SaveSpecifyLayers(refs []name.Digest, path string, img v1.Image, options ...Option) error {
	tw, err := os.Create(path)
	if err != nil {
		return err
	}
	defer tw.Close()

	tf := tar.NewWriter(tw)
	defer tf.Close()

	var layers []v1.Descriptor
	for _, ref := range refs {
		l, err := Layer(ref, options...)
		if err != nil {
			return err
		}

		d, err := l.Digest()
		if err != nil {
			return err
		}

		mt, err := l.MediaType()
		if err != nil {
			return err
		}

		size, err := l.Size()
		if err != nil {
			return err
		}

		layers = append(layers, v1.Descriptor{MediaType: mt, Size: size, Digest: d})

		layerFile := fmt.Sprintf("%s.tar.gz", d.Hex)

		blob, err := l.Compressed()
		if err != nil {
			return err
		}

		blobSize, err := l.Size()
		if err != nil {
			return err
		}
		if err := writeTarEntry(tf, layerFile, blob, blobSize); err != nil {
			return err
		}
	}

	// The complete manifest.json, which includes incremental layers.
	mfBytes, err := img.RawManifest()
	if err != nil {
		return err
	}
	if err := writeTarEntry(tf, "manifest.json", bytes.NewReader(mfBytes), int64(len(mfBytes))); err != nil {
		return err
	}

	// The complete config.json, which also includes incremental layers
	cfgFile, err := img.RawConfigFile()
	if err != nil {
		return err
	}
	cfgName, err := img.ConfigName()
	if err != nil {
		return err
	}
	cfgFileName := fmt.Sprintf("%s.json", cfgName.Hex)
	if err := writeTarEntry(tf, cfgFileName, bytes.NewReader(cfgFile), int64(len(cfgFile))); err != nil {
		return err
	}

	dBytes, err := json.Marshal(layers)
	if err != nil {
		return err
	}

	// The incremental layer stored in digests.json
	if err := writeTarEntry(tf, "digests.json", bytes.NewReader(dBytes), int64(len(dBytes))); err != nil {
		return err
	}
	return nil
}

// writeTarEntry writes a file to the provided writer with a corresponding tar header
func writeTarEntry(tf *tar.Writer, path string, r io.Reader, size int64) error {
	hdr := &tar.Header{
		Mode:     0644,
		Typeflag: tar.TypeReg,
		Size:     size,
		Name:     path,
	}
	if err := tf.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := io.Copy(tf, r)
	return err
}
