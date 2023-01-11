package controllers

import (
	"context"
	"time"

	"github.com/go-errors/errors"
	"github.com/go-logr/logr"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	xjoin "github.com/redhatinsights/xjoin-operator/api/v1alpha1"
	"github.com/redhatinsights/xjoin-operator/controllers/common"
	"github.com/redhatinsights/xjoin-operator/controllers/components"
	"github.com/redhatinsights/xjoin-operator/controllers/config"
	. "github.com/redhatinsights/xjoin-operator/controllers/datasource"
	"github.com/redhatinsights/xjoin-operator/controllers/kafka"
	xjoinlogger "github.com/redhatinsights/xjoin-operator/controllers/log"
	"github.com/redhatinsights/xjoin-operator/controllers/parameters"
	"github.com/redhatinsights/xjoin-operator/controllers/schemaregistry"
	k8sUtils "github.com/redhatinsights/xjoin-operator/controllers/utils"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const xjoindatasourcesynchronizerFinalizer = "finalizer.xjoin.datasourcesynchronizer.cloud.redhat.com"

type XJoinDataSourceSynchronizerReconciler struct {
	Client    client.Client
	Log       logr.Logger
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	Namespace string
	Test      bool
}

func NewXJoinDataSourceSynchronizerReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	log logr.Logger,
	recorder record.EventRecorder,
	namespace string,
	isTest bool) *XJoinDataSourceSynchronizerReconciler {

	return &XJoinDataSourceSynchronizerReconciler{
		Client:    client,
		Log:       log,
		Scheme:    scheme,
		Recorder:  recorder,
		Namespace: namespace,
		Test:      isTest,
	}
}

func (r *XJoinDataSourceSynchronizerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("xjoin-datasourcesynchronizer-controller").
		For(&xjoin.XJoinDataSourceSynchronizer{}).
		WithLogger(mgr.GetLogger()).
		WithOptions(controller.Options{
			Log:         mgr.GetLogger(),
			RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond, 1*time.Minute),
		}).
		Complete(r)
}

// +kubebuilder:rbac:groups=xjoin.cloud.redhat.com,resources=xjoindatasourcesynchronizers;xjoindatasourcesynchronizers/status;xjoindatasourcesynchronizers/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkaconnectors;kafkaconnectors/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkatopics;kafkatopics/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkaconnects;kafkas,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;pods;deployments,verbs=get;list;watch

func (r *XJoinDataSourceSynchronizerReconciler) Reconcile(ctx context.Context, request ctrl.Request) (result ctrl.Result, err error) {
	reqLogger := xjoinlogger.NewLogger("controller_xjoindatasourcesynchronizer", "DataSourceSynchronizer", request.Name, "Namespace", request.Namespace)
	reqLogger.Info("Reconciling XJoinDataSourceSynchronizer")

	instance, err := k8sUtils.FetchXJoinDataSourceSynchronizer(r.Client, request.NamespacedName, ctx)
	if err != nil {
		if k8errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return result, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	p := parameters.BuildDataSourceParameters()

	configManager, err := config.NewManager(config.ManagerOptions{
		Client:         r.Client,
		Parameters:     p,
		ConfigMapNames: []string{"xjoin-generic"},
		SecretNames:    nil,
		Namespace:      instance.Namespace,
		Spec:           instance.Spec,
		Context:        ctx,
		Log:            reqLogger,
	})
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}
	err = configManager.Parse()
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	if p.Pause.Bool() == true {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	i := XJoinDataSourceSynchronizerIteration{
		Parameters: *p,
		Iteration: common.Iteration{
			Context:          ctx,
			Instance:         instance,
			OriginalInstance: instance.DeepCopy(),
			Client:           r.Client,
			Log:              reqLogger,
		},
	}

	if err = i.AddFinalizer(xjoindatasourcesynchronizerFinalizer); err != nil {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	kafkaClient := kafka.GenericKafka{
		Context:          ctx,
		ConnectNamespace: p.ConnectClusterNamespace.String(),
		ConnectCluster:   p.ConnectCluster.String(),
		KafkaNamespace:   p.KafkaClusterNamespace.String(),
		KafkaCluster:     p.KafkaCluster.String(),
		Client:           i.Client,
		Test:             r.Test,
	}

	registry := schemaregistry.NewSchemaRegistryConfluentClient(
		schemaregistry.ConnectionParams{
			Protocol: p.SchemaRegistryProtocol.String(),
			Hostname: p.SchemaRegistryHost.String(),
			Port:     p.SchemaRegistryPort.String(),
		})

	registry.Init()

	componentManager := components.NewComponentManager(common.DataSourceSynchronizerGVK.Kind+"."+instance.Spec.Name, p.Version.String())
	componentManager.AddComponent(components.NewAvroSchema(components.AvroSchemaParameters{
		Schema:   p.AvroSchema.String(),
		Registry: registry,
	}))

	kafkaTopics := kafka.StrimziTopics{
		TopicParameters: kafka.TopicParameters{
			Replicas:           p.KafkaTopicReplicas.Int(),
			Partitions:         p.KafkaTopicPartitions.Int(),
			CleanupPolicy:      p.KafkaTopicCleanupPolicy.String(),
			MinCompactionLagMS: p.KafkaTopicMinCompactionLagMS.String(),
			RetentionBytes:     p.KafkaTopicRetentionBytes.String(),
			RetentionMS:        p.KafkaTopicRetentionMS.String(),
			MessageBytes:       p.KafkaTopicMessageBytes.String(),
			CreationTimeout:    p.KafkaTopicCreationTimeout.Int(),
		},
		KafkaClusterNamespace: p.KafkaClusterNamespace.String(),
		KafkaCluster:          p.KafkaCluster.String(),
		Client:                i.Client,
		Test:                  r.Test,
		Context:               ctx,
		//ResourceNamePrefix:  this is not needed for generic topics
	}
	componentManager.AddComponent(&components.KafkaTopic{
		TopicParameters: kafka.TopicParameters{
			Replicas:           p.KafkaTopicReplicas.Int(),
			Partitions:         p.KafkaTopicPartitions.Int(),
			CleanupPolicy:      p.KafkaTopicCleanupPolicy.String(),
			MinCompactionLagMS: p.KafkaTopicMinCompactionLagMS.String(),
			RetentionBytes:     p.KafkaTopicRetentionBytes.String(),
			RetentionMS:        p.KafkaTopicRetentionMS.String(),
			MessageBytes:       p.KafkaTopicMessageBytes.String(),
			CreationTimeout:    p.KafkaTopicCreationTimeout.Int(),
		},
		KafkaTopics: kafkaTopics,
	})

	componentManager.AddComponent(&components.DebeziumConnector{
		TemplateParameters: config.ParametersToMap(*p),
		KafkaClient:        kafkaClient,
		Template:           p.DebeziumConnectorTemplate.String(),
	})

	if instance.GetDeletionTimestamp() != nil {
		reqLogger.Info("Starting finalizer")
		err = componentManager.DeleteAll()
		if err != nil {
			reqLogger.Error(err, "error deleting components during finalizer")
			return
		}

		controllerutil.RemoveFinalizer(instance, xjoindatasourcesynchronizerFinalizer)
		ctx, cancel := utils.DefaultContext()
		defer cancel()
		err = r.Client.Update(ctx, instance)
		if err != nil {
			return reconcile.Result{}, errors.Wrap(err, 0)
		}

		reqLogger.Info("Successfully finalized")
		return reconcile.Result{}, nil
	}

	err = componentManager.CreateAll()
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	return i.UpdateStatusAndRequeue()
}
