package backuppolicycontroller

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage/names"

	operatorv1 "github.com/openshift/api/operator/v1"
	operatorv1alpha1 "github.com/openshift/api/operator/v1alpha1"
	operatorv1alpha1client "github.com/openshift/client-go/operator/clientset/versioned/typed/operator/v1alpha1"
	operatorv1alpha1listers "github.com/openshift/client-go/operator/listers/operator/v1alpha1"
	"github.com/openshift/cluster-etcd-operator/pkg/backuphelpers"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/health"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/configobserver/featuregates"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/v1helpers"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type BackupPolicyController struct {
	backupsLister         operatorv1alpha1listers.EtcdBackupLister
	backupPoliciesLister  operatorv1alpha1listers.EtcdBackupPolicyLister
	nodeLister            corev1listers.NodeLister
	operatorClient        operatorv1alpha1client.OperatorV1alpha1Interface
	operatorImagePullSpec string
	featureGateAccessor   featuregates.FeatureGateAccess
	eventRecorder         events.Recorder
	cronParser            cron.Parser
}

func NewBackupPolicyController(
	livenessChecker *health.MultiAlivenessChecker,
	backupsLister operatorv1alpha1listers.EtcdBackupLister,
	backupPoliciesLister operatorv1alpha1listers.EtcdBackupPolicyLister,
	nodeLister corev1listers.NodeLister,
	operatorClient operatorv1alpha1client.OperatorV1alpha1Interface,
	staticPodOperatorClient v1helpers.OperatorClient,
	eventRecorder events.Recorder,
	operatorImagePullSpec string,
	accessor featuregates.FeatureGateAccess,
	etcdBackupPolicyInformer factory.Informer,
	etcdBackupInformer factory.Informer,
	nodeInformer cache.SharedIndexInformer) factory.Controller {

	c := &BackupPolicyController{
		backupsLister:         backupsLister,
		backupPoliciesLister:  backupPoliciesLister,
		nodeLister:            nodeLister,
		operatorClient:        operatorClient,
		operatorImagePullSpec: operatorImagePullSpec,
		featureGateAccessor:   accessor,
		eventRecorder:         eventRecorder.WithComponentSuffix("backup-policy-controller"),
		cronParser:            cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}

	syncer := health.NewDefaultCheckingSyncWrapper(c.sync)
	livenessChecker.Add("BackupPolicyController", syncer)

	return factory.New().
		WithInformersQueueKeysFunc(
			func(o runtime.Object) []string {
				if backupPolicy, ok := o.(*operatorv1alpha1.EtcdBackupPolicy); ok {
					return []string{backupPolicy.Name}
				}
				return nil
			},
			etcdBackupPolicyInformer,
		).
		WithBareInformers(
			etcdBackupInformer,
			nodeInformer,
		).
		WithSync(syncer.Sync).
		WithPostStartHooks(func(ctx context.Context, syncCtx factory.SyncContext) error {
			wait.UntilWithContext(ctx, func(ctx context.Context) {
				backupPolicies, err := c.backupPoliciesLister.List(labels.Everything())
				if err != nil {
					klog.Warningf("BackupPolicyController failed to list EtcdBackupPolicies for queueing: %s", err)
					updateControllerDegradedCondition(ctx, staticPodOperatorClient, operatorv1.ConditionTrue, "Error")
					return
				}

				for _, backupPolicy := range backupPolicies {
					syncCtx.Queue().Add(backupPolicy.Name)
				}
				updateControllerDegradedCondition(ctx, staticPodOperatorClient, operatorv1.ConditionFalse, "AsExpected")
			}, 1*time.Minute)
			return nil
		}).
		ToController("BackupPolicyController", eventRecorder.WithComponentSuffix("backup-policy-controller"))
}

func (c *BackupPolicyController) sync(ctx context.Context, syncCtx factory.SyncContext) error {
	if enabled, err := backuphelpers.AutoBackupFeatureGateEnabled(c.featureGateAccessor); !enabled {
		if err != nil {
			klog.V(4).Infof("BackupPolicyController error while checking feature flags: %v", err)
		}
		return nil
	}

	backupPolicyName := syncCtx.QueueKey()
	backupPolicy, err := c.backupPoliciesLister.Get(backupPolicyName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("BackupPolicyController could not get EtcdBackupPolicy %s: %w", backupPolicyName, err)
	} else if backupPolicy.DeletionTimestamp != nil {
		return nil
	}

	schedule, err := c.parseSchedule(backupPolicy)
	if err != nil {
		return fmt.Errorf("BackupPolicyController failed to parse %s schedule: %w", backupPolicyName, err)
	}

	var lastScheduleTime time.Time
	if backupPolicy.Status.LastScheduleTime != nil {
		lastScheduleTime = backupPolicy.Status.LastScheduleTime.Time
	} else {
		lastScheduleTime = backupPolicy.CreationTimestamp.Time
	}
	nextTime := schedule.Next(lastScheduleTime)
	if time.Now().After(nextTime) && !c.hasActiveBackup(ctx, backupPolicy) {
		if err := c.executeBackup(ctx, backupPolicy); err != nil {
			return fmt.Errorf("BackupPolicyController failed to execute backup for EtcdBackupPolicy %s: %w", backupPolicy.Name, err)
		}
	}
	return nil
}

// parseSchedule parses the cron schedule with timezone support
func (c *BackupPolicyController) parseSchedule(backupPolicy *operatorv1alpha1.EtcdBackupPolicy) (cron.Schedule, error) {
	spec := backupPolicy.Spec

	schedule := spec.Schedule
	if spec.TimeZone != "" {
		schedule = fmt.Sprintf("TZ=%s %s", spec.TimeZone, spec.Schedule)
	}

	return c.cronParser.Parse(schedule)
}

func (c *BackupPolicyController) hasActiveBackup(ctx context.Context, backupPolicy *operatorv1alpha1.EtcdBackupPolicy) bool {
	// Live API call to ensure no EtcdBackups are missed
	backups, err := c.operatorClient.EtcdBackups().List(ctx, v1.ListOptions{
		LabelSelector: backuphelpers.LabelEtcdBackupPolicy + "=" + backupPolicy.Name,
	})
	if err != nil {
		klog.V(4).Infof("BackupPolicyController failed to list EtcdBackups for EtcdBackupPolicy %s: %s", backupPolicy.Name, err)
		return true
	}

	for _, backup := range backups.Items {
		// TODO: Maybe should have status.Phase to simplify this
		if !v1helpers.IsConditionTrue(backup.Status.Conditions, string(operatorv1alpha1.BackupCompleted)) && !v1helpers.IsConditionTrue(backup.Status.Conditions, string(operatorv1alpha1.BackupFailed)) {
			return true
		}
	}

	return false
}

// executeBackup creates EtcdBackup resources for each master node
func (c *BackupPolicyController) executeBackup(ctx context.Context, backupPolicy *operatorv1alpha1.EtcdBackupPolicy) error {
	// Get master nodes
	var selector labels.Selector
	if len(backupPolicy.Spec.NodeSelector) != 0 {
		selector = labels.SelectorFromSet(backupPolicy.Spec.NodeSelector)
	} else {
		var err error
		selector, err = labels.Parse("node-role.kubernetes.io/master")
		if err != nil {
			return err
		}
	}

	masterNodes, err := c.nodeLister.List(selector)
	if err != nil {
		c.eventRecorder.Warningf("BackupExecutionFailed",
			"Failed to get master nodes for backup %s, will retry on next sync: %v",
			backupPolicy.Name, err)
		return err
	}

	if len(masterNodes) == 0 {
		c.eventRecorder.Warningf("BackupExecutionSkipped",
			"No master nodes found for backup %s, skipping this execution", backupPolicy.Name)
		return nil
	}
	if backupPolicy.Spec.NodeCount > 0 && backupPolicy.Spec.NodeCount <= len(masterNodes) {
		masterNodes = masterNodes[:backupPolicy.Spec.NodeCount]
	}

	// Track failed creations
	failedCreations := []string{}

	now := time.Now()
	backupNamePrefix := etcdBackupNamePrefix(backupPolicy.Name, now)

	// Create EtcdBackup for each selected master node
	for _, node := range masterNodes {
		etcdBackupName := names.SimpleNameGenerator.GenerateName(backupNamePrefix)
		etcdBackup := &operatorv1alpha1.EtcdBackup{
			ObjectMeta: v1.ObjectMeta{
				Name: etcdBackupName,
				Labels: map[string]string{
					backuphelpers.LabelEtcdBackupPolicy: backupPolicy.Name,
				},
				Finalizers: []string{
					backuphelpers.FinalizerEtcdBackup,
				},
			},
			Spec: operatorv1alpha1.EtcdBackupSpec{
				NodeName: node.Name,
				Storage:  backupPolicy.Spec.Storage,
			},
		}

		_, err := c.operatorClient.EtcdBackups().Create(ctx, etcdBackup, v1.CreateOptions{})
		if err != nil {
			failedCreations = append(failedCreations, node.Name)
			klog.Warningf("Failed to create EtcdBackup %s for node %s: %v", etcdBackupName, node.Name, err)
			continue
		}

		klog.V(2).Infof("Created EtcdBackup %s for node %s", etcdBackupName, node.Name)
	}

	// Update Backup status with last execution time
	backupPolicy = backupPolicy.DeepCopy()
	if err := c.updateBackupStatus(ctx, backupPolicy, masterNodes, now); err != nil {
		klog.Warningf("Failed to update backup status: %v", err)
		// Don't fail the backup execution if status update fails
	}

	if len(failedCreations) > 0 {
		c.eventRecorder.Warningf("PartialBackupFailure",
			"Failed to create backups for nodes: %v", failedCreations)
	} else {
		c.eventRecorder.Eventf("BackupScheduled",
			"Created %d EtcdBackup resources for scheduled backup", len(masterNodes))
	}

	return nil
}

// updateBackupStatus updates the Backup status with last execution information
func (c *BackupPolicyController) updateBackupStatus(ctx context.Context, backupPolicy *operatorv1alpha1.EtcdBackupPolicy, nodes []*corev1.Node, scheduleTime time.Time) error {
	lastScheduleNodes := make([]string, len(nodes))
	for i, node := range nodes {
		lastScheduleNodes[i] = node.Name
	}

	backupPolicy.Status.LastScheduleTime = &v1.Time{Time: scheduleTime}
	backupPolicy.Status.LastScheduleNodes = lastScheduleNodes

	_, err := c.operatorClient.EtcdBackupPolicies().UpdateStatus(ctx, backupPolicy, v1.UpdateOptions{})
	return err
}

func updateControllerDegradedCondition(ctx context.Context, operatorClient v1helpers.OperatorClient, status operatorv1.ConditionStatus, reason string) {
	_, _, updateErr := v1helpers.UpdateStatus(ctx, operatorClient, v1helpers.UpdateConditionFn(operatorv1.OperatorCondition{
		Type:   "BackupPolicyControllerDegraded",
		Status: status,
		Reason: reason,
	}))
	if updateErr != nil {
		klog.V(4).Infof("BackupPolicyController error during UpdateStatus: %v", updateErr)
	}
}

func etcdBackupNamePrefix(backupPolicyName string, now time.Time) string {
	timestamp := now.Format("20060102-150405")
	backupNamePrefix := backupPolicyName
	maxLen := names.MaxGeneratedNameLength - len(timestamp) - 2
	if len(backupNamePrefix) > maxLen {
		backupNamePrefix = backupNamePrefix[:maxLen]
	}
	return backupNamePrefix + "-" + timestamp + "-"
}
