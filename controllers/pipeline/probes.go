package synchronizer

import "github.com/redhatinsights/xjoin-operator/controllers/metrics"

func (i *ReconcileIteration) ProbeStartingInitialSync() {
	i.Log.Info("New synchronizer version", "version", i.Instance.Status.SynchronizerVersion)
	// i.EventNormal("InitialSync", "Starting data synchronization to %s", i.instance.Status.TableName)
	i.Log.Info("Transitioning to InitialSync")
}

func (i *ReconcileIteration) ProbeStateDeviationRefresh(reason string) {
	i.Log.Info("Refreshing synchronizer due to state deviation", "reason", reason)
	metrics.SynchronizerRefreshed("deviation")
	i.EventWarning("Refreshing", "Refreshing synchronizer due to state deviation: %s", reason)
}

func (i *ReconcileIteration) ProbeSynchronizerDidNotBecomeValid() {
	i.Log.Info("Synchronizer failed to become valid. Refreshing.")
	i.EventWarning("Refreshing", "Synchronizer failed to become valid within the given threshold")
	metrics.SynchronizerRefreshed("invalid")
}
