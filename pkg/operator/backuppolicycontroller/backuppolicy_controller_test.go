package backuppolicycontroller

import (
	"context"
	"testing"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	operatorv1alpha1 "github.com/openshift/api/operator/v1alpha1"
	operatorfake "github.com/openshift/client-go/operator/clientset/versioned/fake"
	operatorinformers "github.com/openshift/client-go/operator/informers/externalversions"
	"github.com/openshift/cluster-etcd-operator/pkg/backuphelpers"
	"github.com/openshift/cluster-etcd-operator/pkg/testutils"
	"github.com/openshift/library-go/pkg/operator/configobserver/featuregates"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	k8sfakeclient "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"
)

var backupFeatureGateAccessor = featuregates.NewHardcodedFeatureGateAccess(
	[]configv1.FeatureGateName{backuphelpers.AutomatedEtcdBackupFeatureGateName},
	[]configv1.FeatureGateName{})

type testCaseBackupPolicyController struct {
	backupPolicies []*operatorv1alpha1.EtcdBackupPolicy
	backups        []*operatorv1alpha1.EtcdBackup
	nodes          []*corev1.Node
	expectError    bool
	validate       func(t *testing.T, client *k8sfakeclient.Clientset, operatorFake *operatorfake.Clientset)
}

func runBackupPolicyControllerTest(t *testing.T, tc testCaseBackupPolicyController) {
	t.Helper()
	operatorObjs := make([]runtime.Object, 0, len(tc.backupPolicies)+len(tc.backups))
	operatorObjs = testutils.AppendRuntimeObjects(operatorObjs, tc.backupPolicies)
	operatorObjs = testutils.AppendRuntimeObjects(operatorObjs, tc.backups)

	k8sObjs := make([]runtime.Object, 0, len(tc.nodes))
	k8sObjs = testutils.AppendRuntimeObjects(k8sObjs, tc.nodes)

	client := k8sfakeclient.NewSimpleClientset(k8sObjs...)
	operatorFake := operatorfake.NewSimpleClientset(operatorObjs...)

	sharedFactory := informers.NewSharedInformerFactory(client, 0)
	operatorSharedFactory := operatorinformers.NewSharedInformerFactory(operatorFake, 0)

	backupsInformer := operatorSharedFactory.Operator().V1alpha1().EtcdBackups()
	backupPoliciesInformer := operatorSharedFactory.Operator().V1alpha1().EtcdBackupPolicies()
	nodesInformer := sharedFactory.Core().V1().Nodes()

	backupsInformerHasSynced := backupsInformer.Informer().HasSynced
	backupPoliciesInformerHasSynced := backupPoliciesInformer.Informer().HasSynced
	nodesInformerHasSynced := nodesInformer.Informer().HasSynced

	ctx := t.Context()
	sharedFactory.Start(ctx.Done())
	operatorSharedFactory.Start(ctx.Done())
	cache.WaitForCacheSync(ctx.Done(), nodesInformerHasSynced, backupsInformerHasSynced, backupPoliciesInformerHasSynced)

	fakeClock := testingclock.NewFakeClock(time.Now())
	eventRecorder := events.NewInMemoryRecorder("test", fakeClock)
	t.Cleanup(eventRecorder.Shutdown)

	controller := &BackupPolicyController{
		backupsLister:         backupsInformer.Lister(),
		backupPoliciesLister:  backupPoliciesInformer.Lister(),
		nodeLister:            nodesInformer.Lister(),
		operatorClient:        operatorFake.OperatorV1alpha1(),
		operatorImagePullSpec: "pullspec-image",
		featureGateAccessor:   backupFeatureGateAccessor,
		eventRecorder:         eventRecorder,
		cronParser:            cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}

	for _, backupPolicy := range tc.backupPolicies {
		syncCtx := testutils.FakeSyncContext(t, backupPolicy.Name)
		err := controller.sync(ctx, syncCtx)
		if tc.expectError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}

	if tc.validate != nil {
		tc.validate(t, client, operatorFake)
	}
}

func TestBackupPolicyCreateBackup(t *testing.T) {
	runBackupPolicyControllerTest(t, testCaseBackupPolicyController{
		backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
			testutils.FakeEtcdBackupPolicy("test-backup-policy", "@daily", testutils.WithBackupPolicyAge(25*time.Hour))},
		nodes: []*corev1.Node{testutils.FakeNode("test-node", testutils.WithMasterLabel())},
		validate: func(t *testing.T, client *k8sfakeclient.Clientset, operatorFake *operatorfake.Clientset) {
			// Verify new backup was created
			backups, err := operatorFake.OperatorV1alpha1().EtcdBackups().List(context.TODO(), v1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, backups.Items, 1)

			backup := backups.Items[0]
			require.Equal(t, "test-backup-policy", backup.Labels[backuphelpers.LabelEtcdBackupPolicy])
			require.Equal(t, backup.Spec.NodeName, "test-node")

			// Verify LastScheduleTime and LastScheduleNodes are set
			backupPolicy, err := operatorFake.OperatorV1alpha1().EtcdBackupPolicies().Get(context.TODO(), "test-backup-policy", v1.GetOptions{})
			require.NoError(t, err)
			require.NotNil(t, backupPolicy.Status.LastScheduleTime)
			require.Equal(t, backupPolicy.Status.LastScheduleNodes, []string{"test-node"})
		},
	})
}

func TestBackupPolicyCreateMultipleBackupsWithSelector(t *testing.T) {
	withSpecialLabel := func(node *corev1.Node) {
		node.Labels["special"] = "label"
	}
	runBackupPolicyControllerTest(t, testCaseBackupPolicyController{
		backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
			testutils.FakeEtcdBackupPolicy("test-backup-policy", "@daily", testutils.WithBackupPolicyAge(25*time.Hour), func(backup *operatorv1alpha1.EtcdBackupPolicy) {
				backup.Spec.NodeSelector = map[string]string{"special": "label"}
			})},
		nodes: []*corev1.Node{
			testutils.FakeNode("test-node-1", testutils.WithMasterLabel()),
			testutils.FakeNode("test-node-2", testutils.WithMasterLabel(), withSpecialLabel),
			testutils.FakeNode("test-node-3", testutils.WithMasterLabel(), withSpecialLabel)},
		validate: func(t *testing.T, client *k8sfakeclient.Clientset, operatorFake *operatorfake.Clientset) {
			// Verify new backups were created
			backups, err := operatorFake.OperatorV1alpha1().EtcdBackups().List(context.TODO(), v1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, backups.Items, 2)

			expectedNodes := []string{"test-node-2", "test-node-3"}
			backupNodes := make([]string, 2)
			for i, backup := range backups.Items {
				require.Equal(t, "test-backup-policy", backup.Labels[backuphelpers.LabelEtcdBackupPolicy])
				backupNodes[i] = backup.Spec.NodeName
			}
			require.ElementsMatch(t, backupNodes, expectedNodes)

			backupPolicy, err := operatorFake.OperatorV1alpha1().EtcdBackupPolicies().Get(context.TODO(), "test-backup-policy", v1.GetOptions{})
			require.NoError(t, err)
			require.NotNil(t, backupPolicy.Status.LastScheduleTime)
			require.ElementsMatch(t, backupPolicy.Status.LastScheduleNodes, expectedNodes)
		},
	})
}

func TestBackupPolicyBackupExists(t *testing.T) {
	runBackupPolicyControllerTest(t, testCaseBackupPolicyController{
		backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
			testutils.FakeEtcdBackupPolicy("test-backup-policy", "@daily", testutils.WithBackupPolicyAge(25*time.Hour))},
		backups: []*operatorv1alpha1.EtcdBackup{testutils.FakeEtcdBackup("test-backup", testutils.WithBackupPolicy("test-backup-policy"))},
		validate: func(t *testing.T, client *k8sfakeclient.Clientset, operatorFake *operatorfake.Clientset) {
			// No new etcdbackup was created
			for _, action := range operatorFake.Actions() {
				if _, ok := action.(k8stesting.CreateAction); ok {
					require.Fail(t, "Did not expect to find a new create action")
				}
			}

			backups, err := operatorFake.OperatorV1alpha1().EtcdBackups().List(context.TODO(), v1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, backups.Items, 1)
			require.Equal(t, backups.Items[0].Name, "test-backup")
		},
	})
}

func TestBackupPolicyIgnoreDeleted(t *testing.T) {
	runBackupPolicyControllerTest(t, testCaseBackupPolicyController{
		backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
			testutils.FakeEtcdBackupPolicy("test-backup-policy", "@daily", testutils.WithBackupPolicyAge(25*time.Hour), testutils.WithBackupPolicyDeleted())},
		validate: func(t *testing.T, client *k8sfakeclient.Clientset, operatorFake *operatorfake.Clientset) {
			// No etcdbackup was created
			for _, action := range operatorFake.Actions() {
				if _, ok := action.(k8stesting.CreateAction); ok {
					require.Fail(t, "Did not expect to find a new create action")
				}
			}

			backups, err := operatorFake.OperatorV1alpha1().EtcdBackups().List(context.TODO(), v1.ListOptions{})
			require.NoError(t, err)
			require.Len(t, backups.Items, 0)
		},
	})
}

func TestBackupPolicyScheduleParsing(t *testing.T) {
	testCases := map[string]testCaseBackupPolicyController{
		"valid schedule without timezone": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "0 4 * * *")},
			expectError: false,
		},
		"valid schedule with UTC timezone": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "0 4 * * *", testutils.WithBackupPolicyTimeZone("UTC"))},
			expectError: false,
		},
		"valid schedule with America/New_York timezone": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "0 4 * * *", testutils.WithBackupPolicyTimeZone("America/New_York"))},
			expectError: false,
		},
		"valid named schedule": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "@daily")},
			expectError: false,
		},
		"valid named schedule with America/New_York timezone": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "@every 2h", testutils.WithBackupPolicyTimeZone("America/New_York"))},
			expectError: false,
		},
		"invalid schedule": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "invalid cron")},
			expectError: true,
		},
		"invalid named schedule": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "@invalidcron")},
			expectError: true,
		},
		"invalid timezone": {
			backupPolicies: []*operatorv1alpha1.EtcdBackupPolicy{
				testutils.FakeEtcdBackupPolicy("test-backup-policy", "0 4 * * *", testutils.WithBackupPolicyTimeZone("Invalid/Timezone"))},
			expectError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			runBackupPolicyControllerTest(t, tc)
		})
	}
}
