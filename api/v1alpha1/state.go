package v1alpha1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SynchronizerState string

const validConditionType = "Valid"

const (
	STATE_NEW          SynchronizerState = "NEW"
	STATE_INITIAL_SYNC SynchronizerState = "INITIAL_SYNC"
	STATE_VALID        SynchronizerState = "VALID"
	STATE_INVALID      SynchronizerState = "INVALID"
	STATE_REMOVED      SynchronizerState = "REMOVED"
	STATE_UNKNOWN      SynchronizerState = "UNKNOWN"
)

func (instance *XJoinSynchronizer) GetState() SynchronizerState {
	switch {
	case instance.GetDeletionTimestamp() != nil:
		return STATE_REMOVED
	case instance.Status.SynchronizerVersion == "":
		return STATE_NEW
	case instance.IsValid():
		return STATE_VALID
	case instance.Status.InitialSyncInProgress == true:
		return STATE_INITIAL_SYNC
	case instance.GetValid() == metav1.ConditionFalse:
		return STATE_INVALID
	default:
		return STATE_UNKNOWN
	}
}

func (instance *XJoinSynchronizer) SetValid(status metav1.ConditionStatus, reason string, message string) {
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type:    validConditionType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})

	switch status {
	case metav1.ConditionFalse:
		instance.Status.ValidationFailedCount++
	case metav1.ConditionUnknown:
		instance.Status.ValidationFailedCount = 0
	case metav1.ConditionTrue:
		instance.Status.ValidationFailedCount = 0
		instance.Status.InitialSyncInProgress = false
	}
}

func (instance *XJoinSynchronizer) ResetValid() {
	instance.SetValid(metav1.ConditionUnknown, "New", "Validation not yet run")
}

func (instance *XJoinSynchronizer) IsValid() bool {
	return meta.IsStatusConditionPresentAndEqual(instance.Status.Conditions, validConditionType, metav1.ConditionTrue)
}

func (instance *XJoinSynchronizer) GetValid() metav1.ConditionStatus {
	condition := meta.FindStatusCondition(instance.Status.Conditions, validConditionType)

	if condition == nil {
		return metav1.ConditionUnknown
	}

	return condition.Status
}

func (instance *XJoinSynchronizer) TransitionToInitialSync(resourceNamePrefix string, synchronizerVersion string) error {
	if err := instance.assertState(STATE_INITIAL_SYNC, STATE_INITIAL_SYNC, STATE_NEW); err != nil {
		return err
	}

	instance.ResetValid()
	instance.Status.InitialSyncInProgress = true
	instance.Status.SynchronizerVersion = synchronizerVersion

	return nil
}

func (instance *XJoinSynchronizer) TransitionToNew() {
	instance.ResetValid()
	instance.Status.InitialSyncInProgress = false
	instance.Status.SynchronizerVersion = ""
}

func (instance *XJoinSynchronizer) assertState(targetState SynchronizerState, validStates ...SynchronizerState) error {
	for _, state := range validStates {
		if instance.GetState() == state {
			return nil
		}
	}

	return fmt.Errorf("Attempted invalid state transition from %s to %s", instance.GetState(), targetState)
}
