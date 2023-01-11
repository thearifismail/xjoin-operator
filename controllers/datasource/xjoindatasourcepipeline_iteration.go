package datasource

import (
	"github.com/thearifismail/xjoin-operator/api/v1alpha1"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"github.com/thearifismail/xjoin-operator/controllers/parameters"
)

type XJoinDataSourceSynchronizerIteration struct {
	common.Iteration
	Parameters parameters.DataSourceParameters
}

func (i XJoinDataSourceSynchronizerIteration) GetInstance() *v1alpha1.XJoinDataSourceSynchronizer {
	return i.Instance.(*v1alpha1.XJoinDataSourceSynchronizer)
}
