package datasource

import (
	"github.com/go-errors/errors"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type DataSourceSynchronizerChild struct {
	iteration *XJoinDataSourceIteration
}

func NewDataSourceSynchronizerChild(iteration *XJoinDataSourceIteration) *DataSourceSynchronizerChild {
	return &DataSourceSynchronizerChild{
		iteration: iteration,
	}
}

func (d *DataSourceSynchronizerChild) GetParentInstance() common.XJoinObject {
	return d.iteration.GetInstance()
}

func (d *DataSourceSynchronizerChild) Create(version string) (err error) {
	err = d.iteration.CreateDataSourceSynchronizer(d.iteration.GetInstance().GetName(), version)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (d *DataSourceSynchronizerChild) Delete(version string) (err error) {
	err = d.iteration.DeleteDataSourceSynchronizer(d.iteration.GetInstance().GetName(), version)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (d *DataSourceSynchronizerChild) GetGVK() (gvk schema.GroupVersionKind) {
	return common.DataSourceSynchronizerGVK
}
