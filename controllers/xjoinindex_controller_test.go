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
	//+kubebuilder:scaffold:imports
)

var _ = Describe("XJoinIndex", func() {
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
		It("Should create a XJoinIndexSynchronizer", func() {
			reconciler := IndexTestReconciler{
				Namespace: namespace,
				Name:      "test-index",
				K8sClient: k8sClient,
			}
			createdIndex := reconciler.ReconcileNew()

			indexSynchronizerName := createdIndex.Name + "." + createdIndex.Status.RefreshingVersion

			indexSynchronizerKey := types.NamespacedName{Name: indexSynchronizerName, Namespace: namespace}
			createdIndexSynchronizer := &v1alpha1.XJoinIndexSynchronizer{}
			k8sGet(indexSynchronizerKey, createdIndexSynchronizer)
			Expect(createdIndexSynchronizer.Name).To(Equal(indexSynchronizerName))
			Expect(createdIndexSynchronizer.Spec.Name).To(Equal(createdIndex.Name))
			Expect(createdIndexSynchronizer.Spec.Version).To(Equal(createdIndex.Status.RefreshingVersion))
			Expect(createdIndexSynchronizer.Spec.AvroSchema).To(Equal(createdIndex.Spec.AvroSchema))
			Expect(createdIndexSynchronizer.Spec.Pause).To(Equal(createdIndex.Spec.Pause))
			Expect(createdIndexSynchronizer.Spec.CustomSubgraphImages).To(Equal(createdIndex.Spec.CustomSubgraphImages))

			controller := true
			blockOwnerDeletion := true
			indexOwnerReference := metav1.OwnerReference{
				APIVersion:         "v1alpha1",
				Kind:               "XJoinIndex",
				Name:               createdIndex.Name,
				UID:                createdIndex.UID,
				Controller:         &controller,
				BlockOwnerDeletion: &blockOwnerDeletion,
			}
			Expect(createdIndexSynchronizer.OwnerReferences).To(HaveLen(1))
			Expect(createdIndexSynchronizer.OwnerReferences).To(ContainElement(indexOwnerReference))
		})
	})

	Context("Reconcile Delete", func() {
		It("Should delete a XJoinIndexSynchronizer", func() {
			reconciler := IndexTestReconciler{
				Namespace: namespace,
				Name:      "test-index",
				K8sClient: k8sClient,
			}
			createdIndex := reconciler.ReconcileNew()

			indexSynchronizerList := &v1alpha1.XJoinIndexSynchronizerList{}
			err := k8sClient.List(context.Background(), indexSynchronizerList, client.InNamespace(namespace))
			checkError(err)
			Expect(indexSynchronizerList.Items).To(HaveLen(1))

			err = k8sClient.Delete(context.Background(), &createdIndex)
			checkError(err)
			reconciler.ReconcileDelete()

			err = k8sClient.List(context.Background(), indexSynchronizerList, client.InNamespace(namespace))
			checkError(err)
			Expect(indexSynchronizerList.Items).To(HaveLen(0))

		})
	})
})
