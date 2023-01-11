package controllers

import (
	"context"

	"github.com/jarcoal/httpmock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thearifismail/xjoin-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	// +kubebuilder:scaffold:imports
)

var _ = Describe("XJoinDataSource", func() {
	var namespace string

	BeforeEach(func() {
		httpmock.Activate()
		httpmock.RegisterNoResponder(httpmock.InitialTransport.RoundTrip) //disable mocks for unregistered http requests

		var err error
		namespace, err = NewNamespace()
		checkError(err)
	})

	AfterEach(func() {
		httpmock.DeactivateAndReset()
	})

	Context("Reconcile", func() {
		It("Should create a XJoinDataSourceSynchronizer", func() {
			reconciler := DatasourceTestReconciler{
				Namespace: namespace,
				Name:      "test-data-source",
				K8sClient: k8sClient,
			}
			createdDataSource := reconciler.ReconcileNew()

			dataSourceSynchronizerName := createdDataSource.Name + "." + createdDataSource.Status.RefreshingVersion
			datasourceSynchronizerKey := types.NamespacedName{Name: dataSourceSynchronizerName, Namespace: namespace}
			createdDatasourceSynchronizer := &v1alpha1.XJoinDataSourceSynchronizer{}
			k8sGet(datasourceSynchronizerKey, createdDatasourceSynchronizer)

			Expect(createdDatasourceSynchronizer.Name).To(Equal(dataSourceSynchronizerName))
			Expect(createdDatasourceSynchronizer.Spec.Name).To(Equal(createdDataSource.Name))
			Expect(createdDatasourceSynchronizer.Spec.Version).To(Equal(createdDataSource.Status.RefreshingVersion))
			Expect(createdDatasourceSynchronizer.Spec.AvroSchema).To(Equal(createdDataSource.Spec.AvroSchema))
			Expect(createdDatasourceSynchronizer.Spec.DatabaseHostname).To(Equal(createdDataSource.Spec.DatabaseHostname))
			Expect(createdDatasourceSynchronizer.Spec.DatabasePort).To(Equal(createdDataSource.Spec.DatabasePort))
			Expect(createdDatasourceSynchronizer.Spec.DatabaseUsername).To(Equal(createdDataSource.Spec.DatabaseUsername))
			Expect(createdDatasourceSynchronizer.Spec.DatabasePassword).To(Equal(createdDataSource.Spec.DatabasePassword))
			Expect(createdDatasourceSynchronizer.Spec.DatabaseName).To(Equal(createdDataSource.Spec.DatabaseName))
			Expect(createdDatasourceSynchronizer.Spec.DatabaseTable).To(Equal(createdDataSource.Spec.DatabaseTable))
			Expect(createdDatasourceSynchronizer.Spec.Pause).To(Equal(createdDataSource.Spec.Pause))

			controller := true
			blockOwnerDeletion := true
			dataSourceOwnerReference := metav1.OwnerReference{
				APIVersion:         "v1alpha1",
				Kind:               "XJoinDataSource",
				Name:               createdDataSource.Name,
				UID:                createdDataSource.UID,
				Controller:         &controller,
				BlockOwnerDeletion: &blockOwnerDeletion,
			}
			Expect(createdDatasourceSynchronizer.OwnerReferences).To(HaveLen(1))
			Expect(createdDatasourceSynchronizer.OwnerReferences).To(ContainElement(dataSourceOwnerReference))
		})
	})

	Context("Reconcile Delete", func() {
		It("Should delete a XJoinDataSourceSynchronizer", func() {
			reconciler := DatasourceTestReconciler{
				Namespace: namespace,
				Name:      "test-data-source",
				K8sClient: k8sClient,
			}
			createdDataSource := reconciler.ReconcileNew()

			dataSourceSynchronizerList := &v1alpha1.XJoinDataSourceSynchronizerList{}
			err := k8sClient.List(context.Background(), dataSourceSynchronizerList, client.InNamespace(namespace))
			checkError(err)
			Expect(dataSourceSynchronizerList.Items).To(HaveLen(1))

			err = k8sClient.Delete(context.Background(), &createdDataSource)
			checkError(err)
			reconciler.ReconcileDelete()

			err = k8sClient.List(context.Background(), dataSourceSynchronizerList, client.InNamespace(namespace))
			checkError(err)
			Expect(dataSourceSynchronizerList.Items).To(HaveLen(0))
		})
	})
})
