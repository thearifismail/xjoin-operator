package datasource

import (
	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	"github.com/thearifismail/xjoin-operator/api/v1alpha1"
	"github.com/thearifismail/xjoin-operator/controllers/common"
	"github.com/thearifismail/xjoin-operator/controllers/parameters"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type XJoinDataSourceIteration struct {
	common.Iteration
	Parameters parameters.DataSourceParameters
}

func (i *XJoinDataSourceIteration) CreateDataSourceSynchronizer(name string, version string) (err error) {
	dataSourceSynchronizer := unstructured.Unstructured{}
	dataSourceSynchronizer.Object = map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      name + "." + version,
			"namespace": i.Iteration.Instance.GetNamespace(),
			"labels": map[string]interface{}{
				common.COMPONENT_NAME_LABEL: name,
			},
		},
		"spec": map[string]interface{}{
			"name":             name,
			"version":          version,
			"avroSchema":       i.Parameters.AvroSchema.String(),
			"databaseHostname": i.GetInstance().Spec.DatabaseHostname,
			"databasePort":     i.GetInstance().Spec.DatabasePort,
			"databaseName":     i.GetInstance().Spec.DatabaseName,
			"databaseUsername": i.GetInstance().Spec.DatabaseUsername,
			"databasePassword": i.GetInstance().Spec.DatabasePassword,
			"databaseTable":    i.GetInstance().Spec.DatabaseTable,
			"pause":            i.Parameters.Pause.Bool(),
		},
	}
	dataSourceSynchronizer.SetGroupVersionKind(common.DataSourceSynchronizerGVK)
	err = i.CreateChildResource(dataSourceSynchronizer, common.DataSourceGVK)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinDataSourceIteration) DeleteDataSourceSynchronizer(name string, version string) (err error) {
	err = i.DeleteResource(name+"."+version, common.DataSourceSynchronizerGVK)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinDataSourceIteration) ReconcileSynchronizers() (err error) {
	child := NewDataSourceSynchronizerChild(i)
	err = i.ReconcileChild(child)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (i *XJoinDataSourceIteration) Finalize() (err error) {
	i.Log.Info("Starting finalizer")

	err = i.DeleteAllResourceTypeWithComponentName(common.DataSourceSynchronizerGVK, i.GetInstance().GetName())
	if err != nil {
		return errors.Wrap(err, 0)
	}

	controllerutil.RemoveFinalizer(i.Instance, i.GetFinalizerName())
	ctx, cancel := utils.DefaultContext()
	defer cancel()
	err = i.Client.Update(ctx, i.Instance)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	i.Log.Info("Successfully finalized")
	return nil
}

func (i XJoinDataSourceIteration) GetInstance() *v1alpha1.XJoinDataSource {
	return i.Instance.(*v1alpha1.XJoinDataSource)
}

func (i XJoinDataSourceIteration) GetFinalizerName() string {
	return "finalizer.xjoin.datasource.cloud.redhat.com"
}
