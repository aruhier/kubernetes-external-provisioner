/*
 * Copyright 2021 Johan Fleury <jfleury@arcaik.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package controller

import (
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

// Provisioner is an interface that creates templates for PersistentVolumes and
// can create the volume as a new resource in the infrastructure provider. It
// can also remove the volume it created from the underlying storage provider.
type Provisioner interface {
	Name() string
	Provision(ProvisionOptions) (*v1.PersistentVolume, error)
	Delete(*v1.PersistentVolume) error
}

// Resizer is an optional interface implemented by provisioners to allow
// resizing of PersistentVolumes.
type Resizer interface {
	Resize(*v1.PersistentVolume, int64) error
}

// ProvisionOptions is passed to Provisioner.Provision and contains all the
// necessary information required to provision a volume.
type ProvisionOptions struct {
	// VolumeName is the name of the appropriate PersistentVolume. It can be used
	// to generate the volume name on the underlying storage provider.
	VolumeName string

	// PersistentVolumeClaim is a reference to the claim that lead to
	// provisioning of a new PersistentVolume. Provisioners *must* create a PV
	// that would be matched by this PersistentVolumeClaim (i.e. with required
	// capacity, accessMode, labels matching PersistentVolumeClaim.Spec.Selector
	// and so on).
	PersistentVolumeClaim *v1.PersistentVolumeClaim

	// StorageClass is a reference to the storage class that is used for
	// provisioning for this volume
	StorageClass *storagev1.StorageClass
}
