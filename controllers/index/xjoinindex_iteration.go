package index

import (
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"github.com/thearifismail/xjoin-operator/api/v1alpha1"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"github.com/thearifismail/xjoin-operator/controllers/parameters"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type XJoinIndexIteration struct {
	common.Iteration
	Parameters parameters.IndexParameters
}

func (i *XJoinIndexIteration) CreateIndexSynchronizer(name string, version string) (err error) {
	indexSynchronizer := unstructured.Unstructured{}
	indexSynchronizer.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      name + "." + version,
			"namespace": i.Iteration.Instance.GetNamespace(),
			"labels": map[string]interface{}{
				common.COMPONENT_NAME_LABEL: name,
			},
		},
		"spec": map[string]interface{}{
			"name":                 name,
			"version":              version,
			"avroSchema":           i.Parameters.AvroSchema.String(),
			"pause":                i.Parameters.Pause.Bool(),
			"customSubgraphImages": i.Parameters.CustomSubgraphImages.Value(),
		},
	}
	indexSynchronizer.SetGroupVersionKind(common.IndexSynchronizerGVK)

	err = i.CreateChildResource(indexSynchronizer, common.IndexGVK)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinIndexIteration) CreateIndexValidator(name string, version string) (err error) {
	indexValidator := unstructured.Unstructured{}
	indexValidator.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      name + "." + version,
			"namespace": i.Iteration.Instance.GetNamespace(),
			"labels": map[string]interface{}{
				common.COMPONENT_NAME_LABEL: name,
				"app":                       "xjoin-validator",
			},
		},
		"spec": map[string]interface{}{
			"name":       name,
			"version":    version,
			"avroSchema": i.Parameters.AvroSchema.String(),
			"pause":      i.Parameters.Pause.Bool(),
		},
	}
	indexValidator.SetGroupVersionKind(common.IndexValidatorGVK)
	err = i.CreateChildResource(indexValidator, common.IndexGVK)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinIndexIteration) DeleteIndexSynchronizer(name string, version string) (err error) {
	err = i.DeleteResource(name+"."+version, common.IndexSynchronizerGVK)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinIndexIteration) DeleteIndexValidator(name string, version string) (err error) {
	err = i.DeleteResource(name+"."+version, common.IndexValidatorGVK)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i XJoinIndexIteration) GetInstance() *v1alpha1.XJoinIndex {
	return i.Instance.(*v1alpha1.XJoinIndex)
}

func (i XJoinIndexIteration) GetFinalizerName() string {
	return "finalizer.xjoin.index.cloud.redhat.com"
}

func (i *XJoinIndexIteration) Finalize() (err error) {
	i.Log.Info("Starting finalizer")

	err = i.DeleteAllResourceTypeWithComponentName(common.IndexSynchronizerGVK, i.GetInstance().GetName())
	if err != nil {
		return errors.Wrap(err, 0)
	}

	err = i.DeleteAllResourceTypeWithComponentName(common.IndexValidatorGVK, i.GetInstance().GetName())
	if err != nil {
		return errors.Wrap(err, 0)
	}

	controllerutil.RemoveFinalizer(i.Iteration.Instance, i.GetFinalizerName())

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	err = i.Client.Update(ctx, i.Iteration.Instance)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	i.Log.Info("Successfully finalized")
	return nil
}

func (i *XJoinIndexIteration) ReconcileSynchronizer() (err error) {
	child := NewIndexSynchronizerChild(i)
	err = i.ReconcileChild(child)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinIndexIteration) ReconcileValidator() (err error) {
	child := NewIndexValidatorChild(i)
	err = i.ReconcileChild(child)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinIndexIteration) ReconcileChildren() (err error) {
	err = i.ReconcileSynchronizer()
	if err != nil {
		return errors.Wrap(err, 0)
	}
	err = i.ReconcileValidator()
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}
