package index

import (
	"github.com/thearifismail/xjoin-operator/api/v1alpha1"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"github.com/thearifismail/xjoin-operator/controllers/parameters"
)

type XJoinIndexSynchronizerIteration struct {
	common.Iteration
	Parameters parameters.IndexParameters
}

func (i XJoinIndexSynchronizerIteration) GetInstance() *v1alpha1.XJoinIndexSynchronizer {
	return i.Instance.(*v1alpha1.XJoinIndexSynchronizer)
}
