package kafka

import (
	"context"
	"net/http"

	"github.com/redhatinsights/xjoin-operator/controllers/config"
	logger "github.com/redhatinsights/xjoin-operator/controllers/log"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var log = logger.NewLogger("kafka")

type Kafka struct {
	GenericKafka
	Namespace     string
	OwnerScheme   *runtime.Scheme
	Client        client.Client
	Parameters    config.Parameters
	ParametersMap map[string]interface{}
	Recorder      record.EventRecorder
	Test          bool
}

type GenericKafka struct {
	Context          context.Context
	Client           client.Client
	KafkaNamespace   string
	KafkaCluster     string
	ConnectNamespace string
	ConnectCluster   string
	Test             bool
}

type Topics interface {
	TopicName(synchronizerVersion string) string
	CreateTopic(synchronizerVersion string, dryRun bool) error
	DeleteTopicBySynchronizerVersion(synchronizerVersion string) error
	CheckDeviation(string) (error, error)
	ListTopicNamesForSynchronizerVersion(synchronizerVersion string) ([]string, error)
	DeleteAllTopics() error
	ListTopicNamesForPrefix(prefix string) ([]string, error)
	DeleteTopic(topicName string) error
	GetTopic(topicName string) (interface{}, error)
}

type StrimziTopics struct {
	TopicParameters       TopicParameters
	KafkaClusterNamespace string
	KafkaCluster          string
	ResourceNamePrefix    string
	Client                client.Client
	Test                  bool
	Context               context.Context
}

type ManagedTopicsOptions struct {
	ResourceNamePrefix string
	ClientId           string
	ClientSecret       string
	Hostname           string
	AdminURL           string
	TokenURL           string
	TopicParameters    TopicParameters
}

type ManagedTopics struct {
	Options ManagedTopicsOptions
	client  *http.Client
	baseurl string
}

type Connectors interface {
	newESConnectorResource(synchronizerVersion string) (*unstructured.Unstructured, error)
	newDebeziumConnectorResource(synchronizerVersion string) (*unstructured.Unstructured, error)
	newConnectorResource(
		name string,
		class string,
		connectorConfig map[string]interface{},
		connectorTemplate string) (*unstructured.Unstructured, error)
	GetTaskStatus(connectorName string, taskId float64) (map[string]interface{}, error)
	ListConnectorTasks(connectorName string) ([]map[string]interface{}, error)
	RestartTaskForConnector(connectorName string, taskId float64) error
	verifyTaskIsRunning(task map[string]interface{}, connectorName string) (bool, error)
	RestartConnector(connectorName string) error
	CheckIfConnectIsResponding() (bool, error)
	ListConnectorsREST(prefix string) ([]string, error)
	DeleteConnectorsForSynchronizerVersion(synchronizerVersion string) error
	ListConnectorNamesForSynchronizerVersion(synchronizerVersion string) ([]string, error)
	DeleteAllConnectors(resourceNamePrefix string) error
	GetConnectorStatus(connectorName string) (map[string]interface{}, error)
	IsFailed(connectorName string) (bool, error)
	CreateESConnector(
		synchronizerVersion string,
		dryRun bool) (*unstructured.Unstructured, error)
	CreateDebeziumConnector(
		synchronizerVersion string,
		dryRun bool) (*unstructured.Unstructured, error)
	DebeziumConnectorName(synchronizerVersion string) string
	ESConnectorName(synchronizerVersion string) string
	PauseElasticSearchConnector(synchronizerVersion string) error
	ResumeElasticSearchConnector(synchronizerVersion string) error
	setElasticSearchConnectorPause(synchronizerVersion string, pause bool) error
	CreateDryConnectorByType(conType string, version string) (*unstructured.Unstructured, error)
	updateConnectDepReplicas(newReplicas int64) (currentReplicas int64, err error)
	RestartConnect() error
}

type StrimziConnectors struct {
	Kafka  Kafka
	Topics Topics
}
