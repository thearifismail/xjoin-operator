package v1alpha1

import (
	validation "github.com/redhatinsights/xjoin-go-lib/pkg/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type XJoinIndexSynchronizerSpec struct {
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`

	// +kubebuilder:validation:Required
	Version string `json:"version,omitempty"`

	// +kubebuilder:validation:Required
	AvroSchema string `json:"avroSchema,omitempty"`

	// +optional
	CustomSubgraphImages []CustomSubgraphImage `json:"customSubgraphImages,omitempty"`

	// +optional
	Pause bool `json:"pause,omitempty"`
}

type XJoinIndexSynchronizerStatus struct {
	ValidationResponse validation.ValidationResponse `json:"validationResponse,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=xjoinindexsynchronizer,categories=all

type XJoinIndexSynchronizer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   XJoinIndexSynchronizerSpec   `json:"spec,omitempty"`
	Status XJoinIndexSynchronizerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type XJoinIndexSynchronizerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XJoinIndexSynchronizer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XJoinIndexSynchronizer{}, &XJoinIndexSynchronizerList{})
}
