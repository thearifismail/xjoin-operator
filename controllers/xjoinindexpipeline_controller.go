package controllers

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/go-errors/errors"
	"github.com/go-logr/logr"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	xjoin "github.com/thearifismail/xjoin-operator/api/v1alpha1"
	"github.com/thearifismail/xjoin-operator/controllers/avro"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"github.com/thearifismail/xjoin-operator/controllers/components"
	"github.com/thearifismail/xjoin-operator/controllers/config"
	"github.com/thearifismail/xjoin-operator/controllers/elasticsearch"
	. "github.com/thearifismail/xjoin-operator/controllers/index"
	"github.com/thearifismail/xjoin-operator/controllers/kafka"
	xjoinlogger "github.com/thearifismail/xjoin-operator/controllers/log"
	"github.com/thearifismail/xjoin-operator/controllers/parameters"
	"github.com/thearifismail/xjoin-operator/controllers/schemaregistry"
	k8sUtils "github.com/thearifismail/xjoin-operator/controllers/utils"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const xjoinindexsynchronizerFinalizer = "finalizer.xjoin.indexsynchronizer.cloud.redhat.com"

type XJoinIndexSynchronizerReconciler struct {
	Client    client.Client
	Log       logr.Logger
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	Namespace string
	Test      bool
}

func NewXJoinIndexSynchronizerReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	log logr.Logger,
	recorder record.EventRecorder,
	namespace string,
	isTest bool) *XJoinIndexSynchronizerReconciler {

	return &XJoinIndexSynchronizerReconciler{
		Client:    client,
		Log:       log,
		Scheme:    scheme,
		Recorder:  recorder,
		Namespace: namespace,
		Test:      isTest,
	}
}

func (r *XJoinIndexSynchronizerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("xjoin-indexsynchronizer-controller").
		For(&xjoin.XJoinIndexSynchronizer{}).
		WithLogger(mgr.GetLogger()).
		WithOptions(controller.Options{
			Log:         mgr.GetLogger(),
			RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond, 1*time.Minute),
		}).
		Complete(r)
}

// +kubebuilder:rbac:groups=xjoin.cloud.redhat.com,resources=xjoinindexsynchronizers;xjoinindexsynchronizers/status;xjoinindexsynchronizers/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkaconnectors;kafkaconnectors/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkatopics;kafkatopics/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=kafkaconnects;kafkas,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services;events,verbs=get;list;watch;create;delete;update
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;delete;update

func (r *XJoinIndexSynchronizerReconciler) Reconcile(ctx context.Context, request ctrl.Request) (result ctrl.Result, err error) {
	reqLogger := xjoinlogger.NewLogger("controller_xjoinindexsynchronizer", "IndexSynchronizer", request.Name, "Namespace", request.Namespace)
	reqLogger.Info("Reconciling XJoinIndexSynchronizer")

	instance, err := k8sUtils.FetchXJoinIndexSynchronizer(r.Client, request.NamespacedName, ctx)
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

	if len(instance.OwnerReferences) < 1 {
		return reconcile.Result{}, errors.Wrap(errors.New(
			"Missing OwnerReference from xjoinindexsynchronizer. "+
				"XJoinIndexSynchronizer resources must be managed by an XJoinIndex. "+
				"XJoinIndexSynchronizer cannot be created individually."), 0)
	}

	p := parameters.BuildIndexParameters()

	configManager, err := config.NewManager(config.ManagerOptions{
		Client:         r.Client,
		Parameters:     p,
		ConfigMapNames: []string{"xjoin-generic"},
		SecretNames:    []string{"xjoin-elasticsearch"},
		Namespace:      instance.Namespace,
		Spec:           instance.Spec,
		Context:        ctx,
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

	i := XJoinIndexSynchronizerIteration{
		Parameters: *p,
		Iteration: common.Iteration{
			Context:          ctx,
			Instance:         instance,
			OriginalInstance: instance.DeepCopy(),
			Client:           r.Client,
			Log:              reqLogger,
		},
	}

	if err = i.AddFinalizer(xjoinindexsynchronizerFinalizer); err != nil {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	parametersMap := config.ParametersToMap(*p)

	kafkaClient := kafka.GenericKafka{
		Context:          ctx,
		ConnectNamespace: p.ConnectClusterNamespace.String(),
		ConnectCluster:   p.ConnectCluster.String(),
		KafkaNamespace:   p.KafkaClusterNamespace.String(),
		KafkaCluster:     p.KafkaCluster.String(),
		Client:           i.Client,
		Test:             r.Test,
	}

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
		Client:                r.Client,
		Test:                  r.Test,
		Context:               ctx,
	}

	kafkaTopic := &components.KafkaTopic{
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
	}

	elasticSearchConnection := elasticsearch.GenericElasticSearchParameters{
		Url:        p.ElasticSearchURL.String(),
		Username:   p.ElasticSearchUsername.String(),
		Password:   p.ElasticSearchPassword.String(),
		Parameters: parametersMap,
		Context:    i.Context,
	}
	genericElasticsearch, err := elasticsearch.NewGenericElasticsearch(elasticSearchConnection)
	if err != nil {
		return result, errors.Wrap(err, 0)
	}

	schemaRegistryConnectionParams := schemaregistry.ConnectionParams{
		Protocol: p.SchemaRegistryProtocol.String(),
		Hostname: p.SchemaRegistryHost.String(),
		Port:     p.SchemaRegistryPort.String(),
	}
	confluentClient := schemaregistry.NewSchemaRegistryConfluentClient(schemaRegistryConnectionParams)
	confluentClient.Init()
	registryRestClient := schemaregistry.NewSchemaRegistryRestClient(schemaRegistryConnectionParams)

	indexAvroSchemaParser := avro.IndexAvroSchemaParser{
		AvroSchema:      p.AvroSchema.String(),
		Client:          i.Client,
		Context:         i.Context,
		Namespace:       i.Instance.GetNamespace(),
		Log:             i.Log,
		SchemaRegistry:  confluentClient,
		SchemaNamespace: i.Instance.GetName(),
	}
	indexAvroSchema, err := indexAvroSchemaParser.Parse()
	if err != nil {
		return result, errors.Wrap(err, 0)
	}

	componentManager := components.NewComponentManager(common.IndexSynchronizerGVK.Kind+"."+instance.Spec.Name, p.Version.String())

	if indexAvroSchema.JSONFields != nil {
		componentManager.AddComponent(&components.ElasticsearchSynchronizer{
			GenericElasticsearch: *genericElasticsearch,
			JsonFields:           indexAvroSchema.JSONFields,
		})
	}

	elasticSearchIndexComponent := &components.ElasticsearchIndex{
		GenericElasticsearch: *genericElasticsearch,
		Template:             p.ElasticSearchIndexTemplate.String(),
		Properties:           indexAvroSchema.ESProperties,
		WithSynchronizer:     indexAvroSchema.JSONFields != nil,
	}
	componentManager.AddComponent(elasticSearchIndexComponent)
	componentManager.AddComponent(kafkaTopic)
	componentManager.AddComponent(&components.ElasticsearchConnector{
		Template:           p.ElasticSearchConnectorTemplate.String(),
		KafkaClient:        kafkaClient,
		TemplateParameters: parametersMap,
		Topic:              kafkaTopic.Name(),
	})
	componentManager.AddComponent(components.NewAvroSchema(components.AvroSchemaParameters{
		Schema:   indexAvroSchema.AvroSchemaString,
		Registry: confluentClient,
	}))
	graphqlSchemaComponent := components.NewGraphQLSchema(components.GraphQLSchemaParameters{
		Registry: registryRestClient,
	})
	componentManager.AddComponent(graphqlSchemaComponent)
	componentManager.AddComponent(&components.XJoinCore{
		Client:            i.Client,
		Context:           i.Context,
		SourceTopics:      indexAvroSchema.SourceTopics,
		SinkTopic:         indexAvroSchemaParser.AvroSubjectToKafkaTopic(kafkaTopic.Name()),
		KafkaBootstrap:    p.KafkaBootstrapURL.String(),
		SchemaRegistryURL: p.SchemaRegistryProtocol.String() + "://" + p.SchemaRegistryHost.String() + ":" + p.SchemaRegistryPort.String(),
		Namespace:         i.Instance.GetNamespace(),
		Schema:            indexAvroSchema.AvroSchemaString,
	})
	componentManager.AddComponent(&components.XJoinAPISubGraph{
		Client:                i.Client,
		Context:               i.Context,
		Namespace:             i.Instance.GetNamespace(),
		AvroSchema:            indexAvroSchema.AvroSchemaString,
		Registry:              confluentClient,
		ElasticSearchURL:      p.ElasticSearchURL.String(),
		ElasticSearchUsername: p.ElasticSearchUsername.String(),
		ElasticSearchPassword: p.ElasticSearchPassword.String(),
		ElasticSearchIndex:    elasticSearchIndexComponent.Name(),
		Image:                 "quay.io/ckyrouac/xjoin-api-subgraph:latest", //TODO
		GraphQLSchemaName:     graphqlSchemaComponent.Name(),
	})

	for _, customSubgraphImage := range instance.Spec.CustomSubgraphImages {
		customSubgraphGraphQLSchemaComponent := components.NewGraphQLSchema(components.GraphQLSchemaParameters{
			Registry: registryRestClient,
			Suffix:   customSubgraphImage.Name,
		})
		componentManager.AddComponent(customSubgraphGraphQLSchemaComponent)
		componentManager.AddComponent(&components.XJoinAPISubGraph{
			Client:                i.Client,
			Context:               i.Context,
			Namespace:             i.Instance.GetNamespace(),
			AvroSchema:            indexAvroSchema.AvroSchemaString,
			Registry:              confluentClient,
			ElasticSearchURL:      p.ElasticSearchURL.String(),
			ElasticSearchUsername: p.ElasticSearchUsername.String(),
			ElasticSearchPassword: p.ElasticSearchPassword.String(),
			ElasticSearchIndex:    elasticSearchIndexComponent.Name(),
			Image:                 customSubgraphImage.Image,
			Suffix:                customSubgraphImage.Name,
			GraphQLSchemaName:     customSubgraphGraphQLSchemaComponent.Name(),
		})
	}

	if instance.GetDeletionTimestamp() != nil {
		reqLogger.Info("Starting finalizer")
		err = componentManager.DeleteAll()
		if err != nil {
			reqLogger.Error(err, "error deleting components during finalizer")
			return
		}

		controllerutil.RemoveFinalizer(instance, xjoinindexsynchronizerFinalizer)
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

	// build list of datasources
	dataSources := make(map[string]string)
	for _, ref := range indexAvroSchema.References {
		//get each datasource name and resource version
		name := strings.Split(ref.Name, "xjoindatasourcesynchronizer.")[1]
		name = strings.Split(name, ".Value")[0]
		datasourceNamespacedName := types.NamespacedName{
			Name:      name,
			Namespace: i.Instance.GetNamespace(),
		}
		datasource, err := k8sUtils.FetchXJoinDataSource(i.Client, datasourceNamespacedName, ctx)
		if err != nil {
			return reconcile.Result{}, errors.Wrap(err, 0)
		}
		dataSources[name] = datasource.ResourceVersion
	}

	// update parent status
	indexNamespacedName := types.NamespacedName{
		Name:      instance.OwnerReferences[0].Name,
		Namespace: i.Instance.GetNamespace(),
	}
	xjoinIndex, err := k8sUtils.FetchXJoinIndex(i.Client, indexNamespacedName, ctx)
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, 0)
	}

	if !reflect.DeepEqual(xjoinIndex.Status.DataSources, dataSources) {
		xjoinIndex.Status.DataSources = dataSources

		if err := i.Client.Status().Update(ctx, xjoinIndex); err != nil {
			if k8errors.IsConflict(err) {
				i.Log.Error(err, "Status conflict")
				return reconcile.Result{}, err
			}

			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{RequeueAfter: time.Second * 30}, nil
}
