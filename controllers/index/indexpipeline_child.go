package index

import (
	"github.com/go-errors/errors"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type IndexSynchronizerChild struct {
	iteration *XJoinIndexIteration
}

func NewIndexSynchronizerChild(iteration *XJoinIndexIteration) *IndexSynchronizerChild {
	return &IndexSynchronizerChild{
		iteration: iteration,
	}
}

func (d *IndexSynchronizerChild) GetParentInstance() common.XJoinObject {
	return d.iteration.GetInstance()
}

func (d *IndexSynchronizerChild) Create(version string) (err error) {
	err = d.iteration.CreateIndexSynchronizer(d.iteration.GetInstance().GetName(), version)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (d *IndexSynchronizerChild) Delete(version string) (err error) {
	err = d.iteration.DeleteIndexSynchronizer(d.iteration.GetInstance().GetName(), version)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (d *IndexSynchronizerChild) GetGVK() (gvk schema.GroupVersionKind) {
	return common.IndexSynchronizerGVK
}
