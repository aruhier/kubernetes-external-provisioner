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
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"gitlab.com/Arcaik/external-provisioner/internal/loglevel"
)

type persistentVolumeClaimReconciler struct {
	client.Client
	scheme      *runtime.Scheme
	reader      client.Reader
	recorder    record.EventRecorder
	log         logr.Logger
	provisioner Provisioner
	finalizer   string
}

func registerPersistentVolumeClaimReconciler(mgr ctrl.Manager, provisioner Provisioner, finalizer string) error {
	r := &persistentVolumeClaimReconciler{
		Client:      mgr.GetClient(),
		scheme:      mgr.GetScheme(),
		reader:      mgr.GetAPIReader(),
		recorder:    mgr.GetEventRecorderFor(provisioner.Name()),
		log:         ctrl.Log.WithName("controller").WithName("persistentvolumeclaim"),
		provisioner: provisioner,
		finalizer:   finalizer,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.PersistentVolumeClaim{}).
		WithEventFilter(r.filterPersistentVolumeClaim()).
		Complete(r)
}

func (r *persistentVolumeClaimReconciler) filterPersistentVolumeClaim() predicate.Predicate {
	filter := func(claim *v1.PersistentVolumeClaim) bool {
		log := r.log.WithValues("persistentvolumeclaim", fmt.Sprintf("%s/%s", claim.Namespace, claim.Name))

		provisioner, found := claim.Annotations["volume.kubernetes.io/storage-provisioner"]
		if !found {
			log.V(loglevel.Debug).Info("Couldn’t find `volume.kubernetes.io/storage-provisioner` annotation, falling back to `volume.beta.kubernetes.io/storage-provisioner`")

			provisioner, found = claim.Annotations["volume.beta.kubernetes.io/storage-provisioner"]
			if !found {
				log.V(loglevel.Debug).Info("Skipping reconcilation", "reason", "Couldn’t find `volume.kubernetes.io/storage-provisioner` nor `volume.beta.kubernetes.io/storage-provisioner` annotations")
				return false
			}
		}

		if provisioner != r.provisioner.Name() {
			log.V(loglevel.Debug).Info("Skipping reconcilation", "reason", "Provisionner name doesn’t match")
			return false
		}

		return true
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			claim, ok := e.Object.(*v1.PersistentVolumeClaim)
			if !ok {
				return false
			}

			return filter(claim)
		},
		DeleteFunc: func(e event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			claim, ok := e.ObjectNew.(*v1.PersistentVolumeClaim)
			if !ok {
				return false
			}

			return filter(claim)
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func (r *persistentVolumeClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("persistentvolumeclaim", req.NamespacedName.String())
	log.V(loglevel.Debug).Info("Starting reconciliation")

	claim := &v1.PersistentVolumeClaim{}
	if err := r.reader.Get(ctx, req.NamespacedName, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		log.Error(err, "Unable to fetch PersistentVolumeClaim")
		return ctrl.Result{}, err
	}

	if !claim.ObjectMeta.DeletionTimestamp.IsZero() {
		log.V(loglevel.Info).Info("Nothing to do (deletion in progress)")
		return ctrl.Result{}, nil
	}

	if shouldProvision, err := r.shouldProvision(ctx, log, claim); err != nil {
		log.Error(err, "Unexpected error while validating PersistentVolumeClaim")
		return ctrl.Result{}, err
	} else if shouldProvision {
		return r.provision(ctx, claim)
	}

	if shouldResize, err := r.shouldResize(ctx, claim); err != nil {
		log.Error(err, "Unexpected error while validating PersistentVolumeClaim")
		return ctrl.Result{}, err
	} else if shouldResize {
		return r.resize(ctx, claim)
	}

	log.V(loglevel.Info).Info("Nothing to do")
	return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Minute}, nil
}

func (r *persistentVolumeClaimReconciler) shouldProvision(ctx context.Context, log logr.Logger, claim *v1.PersistentVolumeClaim) (bool, error) {
	if claim.Spec.VolumeName != "" {
		log.V(loglevel.Debug).Info("Skipping provisioning", "reason", ".spec.volumeName is not set (should be set automatically by Kubernetes)")
		return false, nil
	}

	class, err := r.getStorageClass(ctx, claim)
	if err != nil {
		return false, err
	}

	if class == nil {
		log.V(loglevel.Debug).Info("Skipping provisioning", "reason", "Couldn’t find StorageClass")
		return false, nil
	}

	if class.VolumeBindingMode != nil && *class.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		log.V(loglevel.Warn).Info("Skipping provisioning", "reason", "StorageClasses with `volumeBindingMode: WaitForFirstConsumer` are not yet supported")
		return false, nil
	}

	return true, nil
}

func (r *persistentVolumeClaimReconciler) provision(ctx context.Context, claim *v1.PersistentVolumeClaim) (ctrl.Result, error) {
	r.recorder.Event(claim, v1.EventTypeNormal, "ProvisioningStarted", "Provisioning started")

	claimRef, err := reference.GetReference(r.scheme, claim)
	if err != nil {
		e := fmt.Errorf("Got unexpected error while getting reference to PersistentVolumeClaim: %s", err)
		r.recorder.Event(claim, v1.EventTypeNormal, "ProvisioningFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	class, err := r.getStorageClass(ctx, claim)
	if err != nil {
		e := fmt.Errorf("Unable to get StorageClass from PersistentVolumeClaim: %s", err)
		r.recorder.Event(claim, v1.EventTypeNormal, "ProvisioningFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	volumeName := fmt.Sprintf("pvc-%s", claim.UID)

	options := ProvisionOptions{
		VolumeName:            volumeName,
		StorageClass:          class,
		PersistentVolumeClaim: claim,
	}

	pv, err := r.provisioner.Provision(options)
	if err != nil {
		e := fmt.Errorf("External provisioning failed: %s", err)
		r.recorder.Event(claim, v1.EventTypeWarning, "ProvisioningFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	if pv.ObjectMeta.Annotations == nil {
		pv.ObjectMeta.Annotations = make(map[string]string)
	}

	pv.ObjectMeta.Annotations["pv.kubernetes.io/provisioned-by"] = class.Provisioner
	pv.Spec.ClaimRef = claimRef
	pv.Spec.StorageClassName = class.ObjectMeta.Name

	controllerutil.AddFinalizer(pv, r.finalizer)

	if err := r.Create(ctx, pv); err != nil {
		e := fmt.Errorf("Unable to create PersistentVolume: %s", err)
		r.recorder.Event(claim, v1.EventTypeWarning, "ProvisioningFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	r.recorder.Event(claim, v1.EventTypeNormal, "ProvisioningFinished", fmt.Sprintf("Created PersistentVolume: %s", volumeName))

	return ctrl.Result{}, nil
}

func (r *persistentVolumeClaimReconciler) shouldResize(ctx context.Context, claim *v1.PersistentVolumeClaim) (bool, error) {
	// If PVC is not bound to a PV, there’s nothing to resize
	if claim.Status.Phase != v1.ClaimBound {
		return false, nil
	}

	if resizer, ok := r.provisioner.(Resizer); !ok || resizer == nil {
		return false, nil
	}

	pv, err := r.getPersistentVolumeFromClaim(ctx, claim)
	if err != nil {
		return false, fmt.Errorf("Unable to fetch PersistentVolume: %s", err)
	}

	if pv.Spec.Capacity.Storage().Value() == claim.Spec.Resources.Requests.Storage().Value() {
		return false, nil
	}

	return true, nil
}

func (r *persistentVolumeClaimReconciler) resize(ctx context.Context, claim *v1.PersistentVolumeClaim) (ctrl.Result, error) {
	r.recorder.Event(claim, v1.EventTypeNormal, "ResizingStarted", "Resizing started")

	pv, err := r.getPersistentVolumeFromClaim(ctx, claim)
	if err != nil {
		e := fmt.Errorf("Unable to fetch PersistentVolume from claim: %s", err)
		r.recorder.Event(claim, v1.EventTypeWarning, "ResizingFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	size := claim.Spec.Resources.Requests.Storage()

	resizer := r.provisioner.(Resizer)
	if err := resizer.Resize(pv, size.Value()); err != nil {
		e := fmt.Errorf("Resizing failed: %s", err)
		r.recorder.Event(claim, v1.EventTypeWarning, "ResizingFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	pv.Spec.Capacity[v1.ResourceName(v1.ResourceStorage)] = *size

	if err := r.Update(ctx, pv); err != nil {
		e := fmt.Errorf("Unable to update PersistentVolume: %s", err)
		r.recorder.Event(pv, v1.EventTypeWarning, "ResizingFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	r.recorder.Event(claim, v1.EventTypeNormal, "ResizingFinished", fmt.Sprintf("PersistentVolume resized to %s", size))

	return ctrl.Result{}, nil
}

func (r *persistentVolumeClaimReconciler) getStorageClass(ctx context.Context, claim *v1.PersistentVolumeClaim) (*storagev1.StorageClass, error) {
	name, found := claim.Annotations[v1.BetaStorageClassAnnotation]
	if !found {
		name = *claim.Spec.StorageClassName
	}

	if name == "" {
		return nil, nil
	}

	class := &storagev1.StorageClass{}
	if err := r.Get(ctx, client.ObjectKey{Name: name}, class); err != nil {
		return nil, err
	}

	return class, nil
}

func (r *persistentVolumeClaimReconciler) getPersistentVolumeFromClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) (*v1.PersistentVolume, error) {
	pv := &v1.PersistentVolume{}
	if err := r.reader.Get(ctx, client.ObjectKey{Name: claim.Spec.VolumeName}, pv); err != nil {
		return nil, err
	}

	return pv, nil
}
