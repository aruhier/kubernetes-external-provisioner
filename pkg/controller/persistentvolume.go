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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"gitlab.com/Arcaik/external-provisioner/internal/loglevel"
)

type persistentVolumeReconciler struct {
	client.Client
	reader      client.Reader
	recorder    record.EventRecorder
	log         logr.Logger
	provisioner Provisioner
	finalizer   string
}

func registerPersistentVolumeReconciler(mgr ctrl.Manager, provisioner Provisioner, finalizer string) error {
	r := &persistentVolumeReconciler{
		Client:      mgr.GetClient(),
		reader:      mgr.GetAPIReader(),
		recorder:    mgr.GetEventRecorderFor(provisioner.Name()),
		log:         ctrl.Log.WithName("controller").WithName("persistentvolume"),
		provisioner: provisioner,
		finalizer:   finalizer,
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.PersistentVolume{}).
		WithEventFilter(r.filterPersistentVolume()).
		Complete(r)
}

func (r *persistentVolumeReconciler) filterPersistentVolume() predicate.Predicate {
	filter := func(pv *v1.PersistentVolume) bool {
		provisionedBy, found := pv.Annotations["pv.kubernetes.io/provisioned-by"]
		if !found {
			return false
		}

		return provisionedBy == r.provisioner.Name()
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pv, ok := e.Object.(*v1.PersistentVolume)
			if !ok {
				return false
			}

			return filter(pv)
		},
		DeleteFunc: func(e event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			pv, ok := e.ObjectNew.(*v1.PersistentVolume)
			if !ok {
				return false
			}

			return filter(pv)
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func (r *persistentVolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithValues("persistentvolume", req.NamespacedName.Name)
	log.V(loglevel.Debug).Info("Starting reconciliation")

	pv := &v1.PersistentVolume{}
	if err := r.reader.Get(ctx, req.NamespacedName, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		log.Error(err, "Unable to fetch PersistentVolume")
		return ctrl.Result{}, err
	}

	if r.shouldDelete(pv) {
		return r.delete(ctx, pv)
	}

	log.V(loglevel.Debug).Info("Nothing to do")
	return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Minute}, nil
}

func (r *persistentVolumeReconciler) shouldDelete(pv *v1.PersistentVolume) bool {
	if pv.ObjectMeta.DeletionTimestamp.IsZero() {
		return false
	}

	if !controllerutil.ContainsFinalizer(pv, r.finalizer) {
		return false
	}

	if !(pv.Status.Phase == v1.VolumeReleased || pv.Status.Phase == v1.VolumeAvailable) {
		return false
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimDelete {
		return false
	}

	return true
}

func (r *persistentVolumeReconciler) delete(ctx context.Context, pv *v1.PersistentVolume) (ctrl.Result, error) {
	r.recorder.Event(pv, v1.EventTypeNormal, "DeletionStarted", "Deletion started")

	if err := r.provisioner.Delete(pv); err != nil {
		e := fmt.Errorf("External deletion failed: %s", err)
		r.recorder.Event(pv, v1.EventTypeWarning, "DeletionFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	controllerutil.RemoveFinalizer(pv, r.finalizer)

	if err := r.Update(ctx, pv); err != nil {
		e := fmt.Errorf("Unable to update PersistentVolume: %s", err)
		r.recorder.Event(pv, v1.EventTypeWarning, "DeletionFailed", e.Error())
		return ctrl.Result{Requeue: true}, e
	}

	return ctrl.Result{}, nil
}
