package common

import "k8s.io/apimachinery/pkg/runtime/schema"

var IndexGVK = schema.GroupVersionKind{
	Group:   "xjoin.cloud.redhat.com",
	Kind:    "XJoinIndex",
	Version: "v1alpha1",
}

var IndexSynchronizerGVK = schema.GroupVersionKind{
	Group:   "xjoin.cloud.redhat.com",
	Kind:    "XJoinIndexSynchronizer",
	Version: "v1alpha1",
}

var IndexValidatorGVK = schema.GroupVersionKind{
	Group:   "xjoin.cloud.redhat.com",
	Kind:    "XJoinIndexValidator",
	Version: "v1alpha1",
}

var DataSourceGVK = schema.GroupVersionKind{
	Group:   "xjoin.cloud.redhat.com",
	Kind:    "XJoinDataSource",
	Version: "v1alpha1",
}

var DataSourceSynchronizerGVK = schema.GroupVersionKind{
	Group:   "xjoin.cloud.redhat.com",
	Kind:    "XJoinDataSourceSynchronizer",
	Version: "v1alpha1",
}

var DeploymentGVK = schema.GroupVersionKind{
	Group:   "apps",
	Kind:    "Deployment",
	Version: "v1",
}

var ServiceGVK = schema.GroupVersionKind{
	Group:   "",
	Kind:    "Service",
	Version: "v1",
}
