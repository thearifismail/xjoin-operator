package index

import (
	"github.com/redhatinsights/xjoin-operator/api/v1alpha1"
	"github.com/redhatinsights/xjoin-operator/controllers/common"
	"github.com/redhatinsights/xjoin-operator/controllers/parameters"
)

type XJoinIndexSynchronizerIteration struct {
	common.Iteration
	Parameters parameters.IndexParameters
}

func (i XJoinIndexSynchronizerIteration) GetInstance() *v1alpha1.XJoinIndexSynchronizer {
	return i.Instance.(*v1alpha1.XJoinIndexSynchronizer)
}
