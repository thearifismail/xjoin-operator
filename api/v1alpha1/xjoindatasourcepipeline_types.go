package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type XJoinDataSourceSynchronizerSpec struct {
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Required
	Version string `json:"version,omitempty"`

	// +kubebuilder:validation:Required
	AvroSchema string `json:"avroSchema,omitempty"`

	// +optional
	DatabaseHostname *StringOrSecretParameter `json:"databaseHostname,omitempty"`

	// +optional
	DatabasePort *StringOrSecretParameter `json:"databasePort,omitempty"`

	// +optional
	DatabaseUsername *StringOrSecretParameter `json:"databaseUsername,omitempty"`

	// +optional
	DatabasePassword *StringOrSecretParameter `json:"databasePassword,omitempty"`

	// +optional
	DatabaseName *StringOrSecretParameter `json:"databaseName,omitempty"`

	// +optional
	DatabaseTable *StringOrSecretParameter `json:"databaseTable,omitempty"`

	// +optional
	Pause bool `json:"pause,omitempty"`
}

type XJoinDataSourceSynchronizerStatus struct {
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=xjoindatasourcesynchronizer,categories=all

type XJoinDataSourceSynchronizer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XJoinDataSourceSynchronizerSpec   `json:"spec,omitempty"`
	Status XJoinDataSourceSynchronizerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type XJoinDataSourceSynchronizerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XJoinDataSourceSynchronizer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XJoinDataSourceSynchronizer{}, &XJoinDataSourceSynchronizerList{})
}
