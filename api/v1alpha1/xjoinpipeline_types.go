/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// XJoinSynchronizerSpec defines the desired state of XJoinSynchronizer
type XJoinSynchronizerSpec struct {
	// +optional
	// +kubebuilder:validation:MinLength:=3
	ResourceNamePrefix *string `json:"resourceNamePrefix,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	KafkaCluster *string `json:"kafkaCluster,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	KafkaClusterNamespace *string `json:"kafkaClusterNamespace,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ConnectCluster *string `json:"connectCluster,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ConnectClusterNamespace *string `json:"connectClusterNamespace,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	HBIDBSecretName *string `json:"hbiDBSecretName,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ElasticSearchSecretName *string `json:"elasticSearchSecretName,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ElasticSearchNamespace *string `json:"elasticSearchNamespace,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ElasticSearchIndexTemplate *string `json:"elasticSearchIndexTemplate,omitempty"`

	// +optional
	Pause bool `json:"pause,omitempty"`

	// +optional
	Ephemeral bool `json:"ephemeral,omitempty"`

	// +optional
	ManagedKafka bool `json:"managedKafka,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ManagedKafkaSecretName *string `json:"managedKafkaSecretName,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	ManagedKafkaSecretNamespace *string `json:"managedKafkaSecretNamespace,omitempty"`

	// +optional
	// +kubebuilder:validation:MinLength:=1
	SchemaRegistrySecretName *string `json:"schemaRegistrySecretName,omitempty"`
}

// XJoinSynchronizerStatus defines the observed state of XJoinSynchronizer
type XJoinSynchronizerStatus struct {
	// +kubebuilder:validation:Minimum:=0
	ValidationFailedCount       int                `json:"validationFailedCount"`
	SynchronizerVersion         string             `json:"synchronizerVersion"`
	XJoinConfigVersion          string             `json:"xjoinConfigVersion"`
	ElasticSearchSecretVersion  string             `json:"elasticsearchSecretVersion"`
	HBIDBSecretVersion          string             `json:"hbiDBSecretVersion"`
	InitialSyncInProgress       bool               `json:"initialSyncInProgress"`
	Conditions                  []metav1.Condition `json:"conditions"`
	ActiveIndexName             string             `json:"activeIndexName"`
	ActiveESConnectorName       string             `json:"activeESConnectorName"`
	ActiveESSynchronizerName    string             `json:"activeESSynchronizerName"`
	ActiveDebeziumConnectorName string             `json:"activeDebeziumConnectorName"`
	ActiveAliasName             string             `json:"activeAliasName"`
	ActiveTopicName             string             `json:"activeTopicName"`
	ActiveReplicationSlotName   string             `json:"activeReplicationSlotName"`
	ActiveSynchronizerVersion   string             `json:"activeSynchronizerVersion"`
	ElasticSearchSecretName     string             `json:"elasticsearchSecretNameName"`
	HBIDBSecretName             string             `json:"hbiDBSecretName"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=xjoin,categories=all

// XJoinSynchronizer is the Schema for the xjoinsynchronizers API
type XJoinSynchronizer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XJoinSynchronizerSpec   `json:"spec,omitempty"`
	Status XJoinSynchronizerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// XJoinSynchronizerList contains a list of XJoinSynchronizer
type XJoinSynchronizerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XJoinSynchronizer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XJoinSynchronizer{}, &XJoinSynchronizerList{})
}
