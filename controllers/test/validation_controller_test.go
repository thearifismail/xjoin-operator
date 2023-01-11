package test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	xjoin "github.com/thearifismail/xjoin-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

var _ = Describe("Validation controller", func() {
	var i *Iteration
	validationSuccessZeroMismatchMessage := "Validation succeeded - 0 hosts IDs (0.00%) do not match, and 0 (0.00%) hosts have inconsistent data."

	BeforeEach(func() {
		iteration, err := Before()
		Expect(err).ToNot(HaveOccurred())
		i = iteration
	})

	AfterEach(func() {
		err := After(i)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("Valid synchronizer", func() {
		It("Correctly validates fully in-sync table", func() {
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion
			_, err = i.SyncHosts(version, 3)
			Expect(err).ToNot(HaveOccurred())

			dbCount, err := i.DbClient.CountHosts()
			Expect(err).ToNot(HaveOccurred())
			Expect(dbCount).To(Equal(3))

			esCount, err := i.EsClient.CountIndex(i.EsClient.ESIndexName(version))
			Expect(err).ToNot(HaveOccurred())
			Expect(esCount).To(Equal(3))

			synchronizer, err = i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationSucceeded"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(validationSuccessZeroMismatchMessage))

		})

		It("Correctly validates fully in-sync initial table", func() {
			err := i.CreateSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.ReconcileXJoin()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion

			err = i.KafkaConnectors.PauseElasticSearchConnector(version)
			Expect(err).ToNot(HaveOccurred())

			_, err = i.SyncHosts(version, 3)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())

			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationSucceeded"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(validationSuccessZeroMismatchMessage))
		})

		It("Correctly validates initial table after a few tries", func() {
			cm := map[string]string{
				"init.validation.percentage.threshold": "0",
				"init.validation.attempts.threshold":   "4",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			err = i.CreateSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.ReconcileXJoin()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion

			err = i.KafkaConnectors.PauseElasticSearchConnector(version)
			Expect(err).ToNot(HaveOccurred())

			id1, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id2, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id3, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ExpectInitSyncInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 3 hosts (100.00%) do not match"))

			err = i.IndexSimpleDocument(version, id1)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectInitSyncInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 2 hosts (66.67%) do not match"))

			err = i.IndexSimpleDocument(version, id2)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectInitSyncInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"ID validation failed - 1 hosts (33.33%) do not match"))

			err = i.IndexSimpleDocument(version, id3)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationSucceeded"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(validationSuccessZeroMismatchMessage))
		})

		It("Correctly validates table after a few tries", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "0",
				"validation.attempts.threshold":   "4",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			_, err = i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.ReconcileXJoin()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion

			err = i.KafkaConnectors.PauseElasticSearchConnector(version)
			Expect(err).ToNot(HaveOccurred())

			id1, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id2, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id3, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ExpectInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 3 hosts (100.00%) do not match"))

			err = i.IndexSimpleDocument(version, id1)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 2 hosts (66.67%) do not match"))

			err = i.IndexSimpleDocument(version, id2)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"ID validation failed - 1 hosts (33.33%) do not match"))

			err = i.IndexSimpleDocument(version, id3)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err = i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationSucceeded"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(validationSuccessZeroMismatchMessage))
		})

		It("Correctly validates synchronizer that's slightly off", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "40",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion

			err = i.KafkaConnectors.PauseElasticSearchConnector(synchronizer.Status.SynchronizerVersion)
			Expect(err).ToNot(HaveOccurred())

			//create 6 hosts in db, 5 in ES
			_, err = i.SyncHosts(version, 5)
			Expect(err).ToNot(HaveOccurred())
			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			dbCount, err := i.DbClient.CountHosts()
			Expect(err).ToNot(HaveOccurred())
			Expect(dbCount).To(Equal(6))

			esCount, err := i.EsClient.CountIndex(i.EsClient.ESIndexName(version))
			Expect(err).ToNot(HaveOccurred())
			Expect(esCount).To(Equal(5))

			synchronizer, err = i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationSucceeded"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Validation succeeded - 1 hosts IDs (16.67%) do not match, and 1 (16.67%) hosts have inconsistent data."))
		})

		It("Correctly validates initial synchronizer that's slightly off", func() {
			cm := map[string]string{
				"init.validation.percentage.threshold": "40",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			err = i.CreateSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.ReconcileXJoin()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion

			err = i.KafkaConnectors.PauseElasticSearchConnector(synchronizer.Status.SynchronizerVersion)
			Expect(err).ToNot(HaveOccurred())

			//create 6 hosts in db, 5 in ES
			_, err = i.SyncHosts(version, 5)
			Expect(err).ToNot(HaveOccurred())
			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			dbCount, err := i.DbClient.CountHosts()
			Expect(err).ToNot(HaveOccurred())
			Expect(dbCount).To(Equal(6))

			esCount, err := i.EsClient.CountIndex(i.EsClient.ESIndexName(version))
			Expect(err).ToNot(HaveOccurred())
			Expect(esCount).To(Equal(5))

			synchronizer, err = i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationSucceeded"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Validation succeeded - 1 hosts IDs (16.67%) do not match, and 1 (16.67%) hosts have inconsistent data."))
		})
	})

	Describe("Invalid synchronizer", func() {
		It("Correctly invalidates synchronizer that's way off", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "5",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())

			err = i.KafkaConnectors.PauseElasticSearchConnector(synchronizer.Status.SynchronizerVersion)
			Expect(err).ToNot(HaveOccurred())

			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_INVALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionFalse))
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 3 hosts (100.00%) do not match"))
		})

		It("Correctly invalidates synchronizer that's somewhat off", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "19",
				"validation.attempts.threshold":   "1",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			version := synchronizer.Status.SynchronizerVersion

			err = i.KafkaConnectors.PauseElasticSearchConnector(version)
			Expect(err).ToNot(HaveOccurred())

			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id1, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id2, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id3, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			id4, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			err = i.IndexSimpleDocument(version, id1)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexSimpleDocument(version, id2)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexSimpleDocument(version, id3)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexSimpleDocument(version, id4)
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_INVALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionFalse))
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"ID validation failed - 1 hosts (20.00%) do not match"))
		})

		It("Keeps incrementing ValidationFailedCount if failures persist", func() {
			cm := map[string]string{
				"validation.attempts.threshold": "10",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())

			err = i.KafkaConnectors.PauseElasticSearchConnector(synchronizer.Status.SynchronizerVersion)
			Expect(err).ToNot(HaveOccurred())
			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ExpectInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.ValidationFailedCount).To(Equal(1))
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 1 hosts (100.00%) do not match"))

			synchronizer, err = i.ExpectInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.ValidationFailedCount).To(Equal(2))
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 1 hosts (100.00%) do not match"))

			synchronizer, err = i.ExpectInvalidReconcile()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.Status.ValidationFailedCount).To(Equal(3))
			Expect(synchronizer.Status.Conditions[0].Reason).To(Equal("ValidationFailed"))
			Expect(synchronizer.Status.Conditions[0].Message).To(Equal(
				"Count validation failed - 1 hosts (100.00%) do not match"))
		})
	})

	Describe("Full validation", func() {
		It("Performs a full validation when id validation passes", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "20",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())

			err = i.CreateSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			_, err = i.ReconcileXJoin()
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.ExpectValidReconcile()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			for j := 0; j < 5; j++ {
				hostId, err := i.InsertSimpleHost()
				Expect(err).ToNot(HaveOccurred())
				err = i.IndexSimpleDocument(synchronizer.Status.SynchronizerVersion, hostId)
				Expect(err).ToNot(HaveOccurred())
			}
			_, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(5)
			Expect(err).ToNot(HaveOccurred())
		})

		It("Skips full validation when id validation fails", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "5",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			err = i.KafkaConnectors.PauseElasticSearchConnector(synchronizer.Status.SynchronizerVersion)
			Expect(err).ToNot(HaveOccurred())

			hostId1, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			hostId2, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexSimpleDocument(synchronizer.Status.SynchronizerVersion, hostId1)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexSimpleDocument(synchronizer.Status.SynchronizerVersion, hostId2)
			Expect(err).ToNot(HaveOccurred())
			_, err = i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_INVALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionFalse))

			recorder, _ := i.ValidationReconciler.Recorder.(*record.FakeRecorder)
			//2 events - one for count validation, one for id validation
			//no event for FullValidation as it should be skipped
			Expect(recorder.Events).To(HaveLen(2))

			//the failure events
			Expect(<-recorder.Events).To(Equal("Normal CountValidationPassed Results: mismatchRatio: 0.3333333333333333, esCount: 2, hbiCount: 3"))
			Expect(<-recorder.Events).To(Equal("Normal IDValidationFailed 1 hosts ids do not match. Number of hosts IDs retrieved: HBI: 3, ES: 2"))
		})

		It("Sets the synchronizer invalid when full validation fails", func() {
			cm := map[string]string{
				"validation.percentage.threshold": "5",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			err = i.KafkaConnectors.PauseElasticSearchConnector(synchronizer.Status.SynchronizerVersion)
			Expect(err).ToNot(HaveOccurred())

			hostId1, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			hostId2, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexSimpleDocument(synchronizer.Status.SynchronizerVersion, hostId1)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexDocumentNow(synchronizer.Status.SynchronizerVersion, hostId2, "display-name-changed")
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_INVALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionFalse))

			recorder, _ := i.ValidationReconciler.Recorder.(*record.FakeRecorder)
			Expect(recorder.Events).To(HaveLen(3))

			//failure events
			msg := <-recorder.Events
			Expect(msg).To(Equal("Normal CountValidationPassed Results: mismatchRatio: 0, esCount: 2, hbiCount: 2"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal IDValidationPassed 0 hosts ids do not match. Number of hosts IDs retrieved: HBI: 2, ES: 2"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal FullValidationFailed 1 hosts do not match. 2 hosts validated."))
		})

		It("Respects lag compensation parameter", func() {
			cm := map[string]string{
				"validation.lag.compensation.seconds": "10",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())
			_, err = i.SyncHosts(synchronizer.Status.SynchronizerVersion, 4)
			Expect(err).ToNot(HaveOccurred())

			//this host should be validated because modified_on is after 10 seconds
			now := time.Now().UTC()
			nowMinus11 := now.Add(-time.Duration(11) * time.Second)
			id, err := i.InsertHost("simple", nowMinus11)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexDocument(synchronizer.Status.SynchronizerVersion, id, "simple", nowMinus11)
			Expect(err).ToNot(HaveOccurred())

			//this host should not be validated because modified on is too recent
			_, err = i.InsertHost("simple", now)
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_VALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionTrue))

			recorder, _ := i.ValidationReconciler.Recorder.(*record.FakeRecorder)
			Expect(recorder.Events).To(HaveLen(3))
			msg := <-recorder.Events
			Expect(msg).To(Equal("Normal CountValidationPassed Results: mismatchRatio: 0.16666666666666666, esCount: 5, hbiCount: 6"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal IDValidationPassed 0 hosts ids do not match. Number of hosts IDs retrieved: HBI: 5, ES: 5"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal FullValidationPassed 0 hosts do not match. 5 hosts validated."))
		})

		It("Respects period parameter", func() {
			cm := map[string]string{
				"validation.period.minutes":           "1",
				"validation.lag.compensation.seconds": "1",
			}
			err := i.CreateConfigMap("xjoin", cm)
			Expect(err).ToNot(HaveOccurred())
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			//these hosts should not be validated because modified_on is older than a minute
			_, err = i.SyncHosts(synchronizer.Status.SynchronizerVersion, 4)
			Expect(err).ToNot(HaveOccurred())

			//this host should be validated because modified_on is less than a minute old
			now := time.Now().UTC()
			nowMinus11 := now.Add(-time.Duration(11) * time.Second)
			id, err := i.InsertHost("simple", nowMinus11)
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexDocument(synchronizer.Status.SynchronizerVersion, id, "simple", nowMinus11)
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_VALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionTrue))

			recorder, _ := i.ValidationReconciler.Recorder.(*record.FakeRecorder)
			Expect(recorder.Events).To(HaveLen(3))
			msg := <-recorder.Events
			Expect(msg).To(Equal("Normal CountValidationPassed Results: mismatchRatio: 0, esCount: 5, hbiCount: 5"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal IDValidationPassed 0 hosts ids do not match. Number of hosts IDs retrieved: HBI: 1, ES: 1"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal FullValidationPassed 0 hosts do not match. 1 hosts validated."))
		})
	})

	Describe("Full validation JSON", func() {
		It("Fails when a top level key is in ES but not in HBI", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-top-level-key-added")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when a top level key is in HBI but not in ES", func() {
			err := i.fullValidationFailureTest("systemprofile-top-level-key-added", "simple")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when a top level key is mismatched", func() {
			err := i.fullValidationFailureTest("systemprofile-top-level-key-added", "systemprofile-top-level-key-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when a simple array has an extra value", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-extra-simple-array-value")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when a simple array has a missing value", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-missing-simple-array-value")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an array of objects has a missing object", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-array-of-objects-missing-object")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an array of objects has an extra object", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-array-of-objects-extra-object")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an array of objects has an extra key in an object", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-array-of-objects-extra-key")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an array of objects has a key missing in an object", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-array-of-objects-missing-key")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an array of objects has a value changed in an object", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-array-of-objects-value-changed")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an object key is added", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-object-key-added")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an object key is removed", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-object-key-removed")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an object value is changed", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-object-value-changed")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an object is missing", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-object-missing")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an array within an object is modified", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-object-array-value-changed")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when a boolean value is modified", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-boolean-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when an integer value is modified", func() {
			err := i.fullValidationFailureTest("simple", "systemprofile-integer-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when there are multiple changes to a single host", func() {
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			_, err = i.SyncHosts(synchronizer.Status.SynchronizerVersion, 5)
			Expect(err).ToNot(HaveOccurred())

			hostId, err := i.InsertSimpleHost()
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexDocumentNow(synchronizer.Status.SynchronizerVersion, hostId, "lots-of-changes")
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_INVALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionFalse))

			recorder, _ := i.ValidationReconciler.Recorder.(*record.FakeRecorder)
			Expect(recorder.Events).To(HaveLen(3))

			msg := <-recorder.Events
			Expect(msg).To(Equal("Normal CountValidationPassed Results: mismatchRatio: 0, esCount: 6, hbiCount: 6"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal IDValidationPassed 0 hosts ids do not match. Number of hosts IDs retrieved: HBI: 6, ES: 6"))
			msg = <-recorder.Events
			Expect(msg).To(Equal("Normal FullValidationFailed 1 hosts do not match. 6 hosts validated."))
		})
	})

	Describe("Tag validation", func() {
		It("Fails when tag structured namespace is inconsistent", func() {
			err := i.fullValidationFailureTest("simple", "tags-structured-namespace-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when tag structured key is inconsistent", func() {
			err := i.fullValidationFailureTest("simple", "tags-structured-key-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when tag structured value is inconsistent", func() {
			err := i.fullValidationFailureTest("simple", "tags-structured-value-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Fails when tag string inconsistent", func() {
			err := i.fullValidationFailureTest("simple", "tags-string-modified")
			Expect(err).ToNot(HaveOccurred())
		})

		It("Validates complex, unordered tags", func() {
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			hostId, err := i.InsertHostNow("tags-multiple-unordered")
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexDocumentNow(synchronizer.Status.SynchronizerVersion, hostId, "tags-multiple-unordered")
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_VALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionTrue))
		})

		It("Correctly validates host with no tags", func() {
			synchronizer, err := i.CreateValidSynchronizer()
			Expect(err).ToNot(HaveOccurred())
			err = i.AssertValidationEvents(0)
			Expect(err).ToNot(HaveOccurred())

			hostId, err := i.InsertHostNow("no-tags")
			Expect(err).ToNot(HaveOccurred())
			err = i.IndexDocumentNow(synchronizer.Status.SynchronizerVersion, hostId, "no-tags")
			Expect(err).ToNot(HaveOccurred())

			synchronizer, err = i.ReconcileValidation()
			Expect(err).ToNot(HaveOccurred())
			Expect(synchronizer.GetState()).To(Equal(xjoin.STATE_VALID))
			Expect(synchronizer.GetValid()).To(Equal(metav1.ConditionTrue))
		})
	})
})
