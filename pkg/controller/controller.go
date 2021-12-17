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
	"fmt"
	"time"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"gitlab.com/Arcaik/external-provisioner/internal/loglevel"
	"gitlab.com/Arcaik/external-provisioner/pkg/version"
)

type options struct {
	logLevel                loglevel.Flag
	leaderElect             bool
	leaderElectionNamespace string
	leaseDuration           time.Duration
	renewDeadline           time.Duration
	retryPeriod             time.Duration
	healthProbeBindAddress  string
	metricsBindAddress      string
}

const (
	defaultLeaderElect                 = true
	defaultLeaderElectionNamespace     = "kube-system"
	defaultLeaderElectionLeaseDuration = 60 * time.Second
	defaultLeaderElectionRenewDeadline = 40 * time.Second
	defaultLeaderElectionRetryPeriod   = 15 * time.Second
	defaultHealthProbeBindAddress      = ":8080"
	defaultMetricsBindAddress          = ":8081"
)

var (
	scheme = runtime.NewScheme()

	o = &options{
		leaderElect:             defaultLeaderElect,
		leaderElectionNamespace: defaultLeaderElectionNamespace,
		leaseDuration:           defaultLeaderElectionLeaseDuration,
		renewDeadline:           defaultLeaderElectionRenewDeadline,
		retryPeriod:             defaultLeaderElectionRetryPeriod,
		healthProbeBindAddress:  defaultHealthProbeBindAddress,
		metricsBindAddress:      defaultMetricsBindAddress,
	}
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

// InitFlags adds flags used to configure the controller on the cobra Command.
func InitFlags(cmd *cobra.Command) error {
	if cmd == nil {
		return fmt.Errorf("Expected a cobra.Command, got nil")
	}

	cmd.SetVersionTemplate(version.Template())

	cmd.PersistentFlags().VarP(&o.logLevel, "v", "v", "Number for the log verbosity level (default 0).")
	cmd.PersistentFlags().BoolVar(&o.leaderElect, "leader-elect", defaultLeaderElect, ""+
		"If true, the controller will perform leader election to ensure no more than one instance "+
		"operates at a time")
	cmd.PersistentFlags().StringVar(&o.leaderElectionNamespace, "leader-election-namespace", defaultLeaderElectionNamespace, ""+
		"Namespace used to perform leader election. This is only applicable if leader election is "+
		"enabled.")
	cmd.PersistentFlags().DurationVar(&o.leaseDuration, "leader-election-lease-duration", defaultLeaderElectionLeaseDuration, ""+
		"The duration that non-leader candidates will wait after observing a leadership renewal until "+
		"attempting to acquire leadership of a led but unrenewed leader slot. This is effectively the "+
		"maximum duration that a leader can be stopped before it is replaced by another candidate. "+
		"This is only applicable if leader election is enabled.")
	cmd.PersistentFlags().DurationVar(&o.renewDeadline, "leader-election-renew-deadline", defaultLeaderElectionRenewDeadline, ""+
		"The interval between attempts by the acting master to renew a leadership slot before it stops "+
		"leading. This must be less than or equal to the lease duration. This is only applicable if "+
		"leader election is enabled.")
	cmd.PersistentFlags().DurationVar(&o.retryPeriod, "leader-election-retry-period", defaultLeaderElectionRetryPeriod, ""+
		"The duration the clients should wait between attempting acquisition and renewal of a "+
		"leadership. This is only applicable if leader election is enabled.")
	cmd.PersistentFlags().StringVar(&o.metricsBindAddress, "metrics-address", defaultMetricsBindAddress, ""+
		"The address that the controller should bind to for serving prometheus metrics.")
	cmd.PersistentFlags().StringVar(&o.healthProbeBindAddress, "health-address", defaultHealthProbeBindAddress, ""+
		"The address that the controller should bind to for serving health probes.")

	return nil
}

// Controller is a controller that watch PersistentVolumeClaim adn provisions
// PersistentVolumes.
type ProvisionController struct {
	mgr ctrl.Manager
}

// NewProvisionController creates a new provision controller with the given Provisioner.
func NewProvisioningController(p Provisioner) (*ProvisionController, error) {
	logger := zap.New(zap.UseDevMode(false), zap.JSONEncoder(), zap.Level(o.logLevel.AtomicLevel())).
		WithName("external-provisioner").
		WithName(p.Name())

	ctrl.SetLogger(logger)

	config, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to get kubeconfig: %s", err)
	}

	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                        scheme,
		LeaderElection:                o.leaderElect,
		LeaderElectionNamespace:       o.leaderElectionNamespace,
		LeaderElectionID:              fmt.Sprintf("external-provisioner-%s", p.Name()),
		LeaderElectionReleaseOnCancel: true,
		LeaseDuration:                 &o.leaseDuration,
		RenewDeadline:                 &o.renewDeadline,
		RetryPeriod:                   &o.retryPeriod,
		HealthProbeBindAddress:        o.healthProbeBindAddress,
		MetricsBindAddress:            o.metricsBindAddress,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to start manager: %s", err)
	}

	finalizer := fmt.Sprintf("%s/finalizer", p.Name())

	if err := registerPersistentVolumeReconciler(mgr, p, finalizer); err != nil {
		return nil, fmt.Errorf("unable to create PersistentVolume controller: %s", err)
	}

	if err := registerPersistentVolumeClaimReconciler(mgr, p, finalizer); err != nil {
		return nil, fmt.Errorf("Unable to create PersistentVolumeClaim controller: %s", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("Unable to set up health check: %s", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return nil, fmt.Errorf("Unable to set up ready check: %s", err)
	}

	return &ProvisionController{mgr: mgr}, nil
}

// Start starts all the controller loop.
//
// The contorller will gracefully stop upon receiving SIGTERM or SIGINT.
func (c *ProvisionController) Start() error {
	return c.mgr.Start(ctrl.SetupSignalHandler())
}
