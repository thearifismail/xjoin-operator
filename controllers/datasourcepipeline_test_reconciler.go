package controllers

import (
	"context"
	"time"

	"github.com/jarcoal/httpmock"
	. "github.com/onsi/gomega"
	"github.com/thearifismail/xjoin-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type DatasourceSynchronizerTestReconciler struct {
	Namespace         string
	Name              string
	K8sClient         client.Client
	createdDatasource v1alpha1.XJoinDataSource
}

func (d *DatasourceSynchronizerTestReconciler) newXJoinDataSourceSynchronizerReconciler() *XJoinDataSourceSynchronizerReconciler {
	return NewXJoinDataSourceSynchronizerReconciler(
		d.K8sClient,
		scheme.Scheme,
		testLogger,
		record.NewFakeRecorder(10),
		d.Namespace,
		true)
}

func (d *DatasourceSynchronizerTestReconciler) createValidDataSourceSynchronizer() {
	ctx := context.Background()

	datasourceSpec := v1alpha1.XJoinDataSourceSynchronizerSpec{
		Name:             d.Name,
		Version:          "1234",
		AvroSchema:       "{}",
		DatabaseHostname: &v1alpha1.StringOrSecretParameter{Value: "dbHost"},
		DatabasePort:     &v1alpha1.StringOrSecretParameter{Value: "8080"},
		DatabaseUsername: &v1alpha1.StringOrSecretParameter{Value: "dbUsername"},
		DatabasePassword: &v1alpha1.StringOrSecretParameter{Value: "dbPassword"},
		DatabaseName:     &v1alpha1.StringOrSecretParameter{Value: "dbName"},
		DatabaseTable:    &v1alpha1.StringOrSecretParameter{Value: "dbTable"},
		Pause:            false,
	}

	datasource := &v1alpha1.XJoinDataSourceSynchronizer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.Name,
			Namespace: d.Namespace,
		},
		Spec: datasourceSpec,
		TypeMeta: metav1.TypeMeta{
			APIVersion: "xjoin.cloud.redhat.com/v1alpha1",
			Kind:       "XJoinDataSourceSynchronizer",
		},
	}

	Expect(d.K8sClient.Create(ctx, datasource)).Should(Succeed())

	// validate datasource spec is created correctly
	datasourceLookupKey := types.NamespacedName{Name: d.Name, Namespace: d.Namespace}
	createdDataSourceSynchronizer := &v1alpha1.XJoinDataSourceSynchronizer{}

	Eventually(func() bool {
		err := d.K8sClient.Get(ctx, datasourceLookupKey, createdDataSourceSynchronizer)
		if err != nil {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

	Expect(createdDataSourceSynchronizer.Spec.Name).Should(Equal(d.Name))
	Expect(createdDataSourceSynchronizer.Spec.Version).Should(Equal("1234"))
	Expect(createdDataSourceSynchronizer.Spec.Pause).Should(Equal(false))
	Expect(createdDataSourceSynchronizer.Spec.AvroSchema).Should(Equal("{}"))
	Expect(createdDataSourceSynchronizer.Spec.DatabaseHostname).Should(Equal(&v1alpha1.StringOrSecretParameter{Value: "dbHost"}))
	Expect(createdDataSourceSynchronizer.Spec.DatabasePort).Should(Equal(&v1alpha1.StringOrSecretParameter{Value: "8080"}))
	Expect(createdDataSourceSynchronizer.Spec.DatabaseUsername).Should(Equal(&v1alpha1.StringOrSecretParameter{Value: "dbUsername"}))
	Expect(createdDataSourceSynchronizer.Spec.DatabasePassword).Should(Equal(&v1alpha1.StringOrSecretParameter{Value: "dbPassword"}))
	Expect(createdDataSourceSynchronizer.Spec.DatabaseName).Should(Equal(&v1alpha1.StringOrSecretParameter{Value: "dbName"}))
	Expect(createdDataSourceSynchronizer.Spec.DatabaseTable).Should(Equal(&v1alpha1.StringOrSecretParameter{Value: "dbTable"}))
}

func (d *DatasourceSynchronizerTestReconciler) ReconcileNew() v1alpha1.XJoinDataSourceSynchronizer {
	d.registerNewMocks()
	d.createValidDataSourceSynchronizer()
	createdDataSourceSynchronizer := &v1alpha1.XJoinDataSourceSynchronizer{}
	result := d.reconcile()
	Expect(result).To(Equal(reconcile.Result{Requeue: false, RequeueAfter: 30000000000}))

	datasourceLookupKey := types.NamespacedName{Name: d.Name, Namespace: d.Namespace}
	Eventually(func() bool {
		err := d.K8sClient.Get(context.Background(), datasourceLookupKey, createdDataSourceSynchronizer)
		if err != nil {
			return false
		}
		return true
	}, K8sGetTimeout, K8sGetInterval).Should(BeTrue())

	return *createdDataSourceSynchronizer
}

func (d *DatasourceSynchronizerTestReconciler) ReconcileDelete() {
	d.registerDeleteMocks()
	result := d.reconcile()
	Expect(result).To(Equal(reconcile.Result{Requeue: false, RequeueAfter: 0}))

	datasourceSynchronizerList := v1alpha1.XJoinDataSourceSynchronizerList{}
	err := d.K8sClient.List(context.Background(), &datasourceSynchronizerList, client.InNamespace(d.Namespace))
	checkError(err)
	Expect(datasourceSynchronizerList.Items).To(HaveLen(0))
}

func (d *DatasourceSynchronizerTestReconciler) reconcile() reconcile.Result {
	xjoinDataSourceSynchronizerReconciler := d.newXJoinDataSourceSynchronizerReconciler()
	datasourceLookupKey := types.NamespacedName{Name: d.Name, Namespace: d.Namespace}
	result, err := xjoinDataSourceSynchronizerReconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: datasourceLookupKey})
	checkError(err)
	return result
}

func (d *DatasourceSynchronizerTestReconciler) registerDeleteMocks() {
	httpmock.Reset()
	httpmock.RegisterNoResponder(httpmock.InitialTransport.RoundTrip) //disable mocks for unregistered http requests

	// avro schema mocks
	httpmock.RegisterResponder(
		"GET",
		"http://apicurio:1080/apis/ccompat/v6/subjects/xjoindatasourcesynchronizer."+d.Name+".1234-value/versions/1",
		httpmock.NewStringResponder(200, `{}`))

	httpmock.RegisterResponder(
		"GET",
		"http://apicurio:1080/apis/ccompat/v6/subjects/xjoindatasourcesynchronizer."+d.Name+".1234-value/versions/latest",
		httpmock.NewStringResponder(200, `{}`))

	httpmock.RegisterResponder(
		"DELETE",
		"http://apicurio:1080/apis/ccompat/v6/subjects/xjoindatasourcesynchronizer."+d.Name+".1234-value",
		httpmock.NewStringResponder(200, `{}`))

	httpmock.RegisterResponder(
		"GET",
		"http://apicurio:1080/apis/registry/v2/groups/default/artifacts/xjoindatasourcesynchronizer."+d.Name+".1234/versions",
		httpmock.NewStringResponder(404, `{}`))

	// kafka connector mocks
	httpmock.RegisterResponder(
		"GET",
		"http://connect-connect-api."+d.Namespace+".svc:8083/connectors/xjoindatasourcesynchronizer."+d.Name+".1234",
		httpmock.NewStringResponder(404, `{}`))
}

func (d *DatasourceSynchronizerTestReconciler) registerNewMocks() {
	httpmock.Reset()
	httpmock.RegisterNoResponder(httpmock.InitialTransport.RoundTrip) //disable mocks for unregistered http requests

	// avro schema mocks
	httpmock.RegisterResponder(
		"GET",
		"http://apicurio:1080/apis/ccompat/v6/subjects/xjoindatasourcesynchronizer."+d.Name+".1234-value/versions/1",
		httpmock.NewStringResponder(404, `{"message":"No version '1' found for artifact with ID 'xjoindatasourcesynchronizer.`+d.Name+`.1234-value' in group 'null'.","error_code":40402}`).Times(1))

	httpmock.RegisterResponder(
		"POST",
		"http://apicurio:1080/apis/ccompat/v6/subjects/xjoindatasourcesynchronizer."+d.Name+".1234-value/versions",
		httpmock.NewStringResponder(200, `{"createdBy":"","createdOn":"2022-07-27T17:28:11+0000","modifiedBy":"","modifiedOn":"2022-07-27T17:28:11+0000","id":1,"version":1,"type":"AVRO","globalId":1,"state":"ENABLED","groupId":"null","contentId":1,"references":[]}`))

	httpmock.RegisterResponder(
		"GET",
		"http://apicurio:1080/apis/ccompat/v6/subjects/xjoindatasourcesynchronizer."+d.Name+".1234-value/versions/latest",
		httpmock.NewStringResponder(200, `{"schema":"{\"name\":\"Value\",\"namespace\":\"xjoindatasourcesynchronizer.`+d.Name+`\"}","schemaType":"AVRO","references":[]}`))
}
