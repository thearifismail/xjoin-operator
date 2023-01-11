package controllers

import (
	"context"
	"time"

	"github.com/go-errors/errors"
	"github.com/go-logr/logr"
	xjoin "github.com/thearifismail/xjoin-operator/api/v1alpha1"
	logger "github.com/thearifismail/xjoin-operator/controllers/log"

	. "github.com/thearifismail/xjoin-operator/controllers/synchronizer"
	// . "github.com/thearifismail/xjoin-operator/controllers/pipeline"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type ValidationReconciler struct {
	XJoinSynchronizerReconciler

	// if true the Reconciler will check for synchronizer state deviation
	// should always be true except for tests
	CheckResourceDeviation bool
}

func (r *ValidationReconciler) setup(reqLogger logger.Log, request ctrl.Request, ctx context.Context) (ReconcileIteration, error) {
	i, err := r.XJoinSynchronizerReconciler.setup(reqLogger, request, ctx)

	if err != nil || i.Instance == nil {
		return i, err
	}

	if i.Instance.Spec.Pause == true {
		return i, nil
	}

	i.GetRequeueInterval = func(i *ReconcileIteration) int {
		return i.GetValidationInterval()
	}

	return i, err
}

func (r *ValidationReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	reqLogger := logger.NewLogger("controller_validation", "Synchronizer", request.Name, "Namespace", request.Namespace)
	reqLogger.Info("Reconciling Validation")

	i, err := r.setup(reqLogger, request, ctx)
	defer i.Close()

	if err != nil {
		i.Error(err)
		return reconcile.Result{}, err
	}

	if i.Instance != nil {
		reqLogger.Debug("Instance State", "state", i.Instance.GetState())
	}

	// nothing to validate
	if i.Instance == nil ||
		i.Instance.GetState() == xjoin.STATE_REMOVED ||
		i.Instance.GetState() == xjoin.STATE_NEW ||
		i.Instance.Spec.Pause == true {

		return reconcile.Result{}, nil
	}

	reqLogger.Info("Checking for resource deviation")

	if r.CheckResourceDeviation {
		reqLogger.Info("Really checking for deviation")
		problem, err := i.CheckForDeviation()
		if err != nil {
			i.Error(err, "Error checking for state deviation")
			return reconcile.Result{}, errors.Wrap(err, 0)
		} else if problem != nil {
			reqLogger.Info("Problem checking for deviation")
			i.ProbeStateDeviationRefresh(problem.Error())
			reqLogger.Info("Transitioning to new state")
			i.Instance.TransitionToNew()
			return i.UpdateStatusAndRequeue()
		}
	}

	reqLogger.Info("Validating XJoinSynchronizer",
		"LagCompensationSeconds", i.Parameters.ValidationLagCompensationSeconds.Int(),
		"ValidationPeriodMinutes", i.Parameters.ValidationPeriodMinutes.Int(),
		"FullValidationEnabled", i.Parameters.FullValidationEnabled.Bool(),
		"FullValidationNumThreads", i.Parameters.FullValidationNumThreads.Int(),
		"FullValidationChunkSize", i.Parameters.FullValidationChunkSize.Int())

	isValid, err := i.Validate()
	if err != nil {
		i.Error(err, "Error validating synchronizer")
		return reconcile.Result{}, err
	}

	reqLogger.Info("Validation finished", "isValid", isValid)

	if isValid {
		if i.Instance.GetState() == xjoin.STATE_INVALID {
			i.EventNormal("Valid", "Synchronizer is valid again")
		}
	}

	return i.UpdateStatusAndRequeue()
}

func eventFilterPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true // Synchronizer definition changed
			}

			oldSynchronizer, ok1 := e.ObjectOld.(*xjoin.XJoinSynchronizer)
			newSynchronizer, ok2 := e.ObjectNew.(*xjoin.XJoinSynchronizer)

			if ok1 && ok2 && oldSynchronizer.Status.InitialSyncInProgress == false && newSynchronizer.Status.InitialSyncInProgress == true {
				return true // synchronizer refresh happened - validate the new synchronizer
			}

			return false
		},
	}
}

func (r *ValidationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("xjoin-validation").
		For(&xjoin.XJoinSynchronizer{}).
		WithEventFilter(eventFilterPredicate()).
		WithLogger(mgr.GetLogger()).
		WithOptions(controller.Options{
			Log:         mgr.GetLogger(),
			RateLimiter: workqueue.NewItemExponentialFailureRateLimiter(time.Millisecond, 1*time.Minute),
		}).
		Complete(r)
}

func NewValidationReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	log logr.Logger,
	checkResourceDeviation bool,
	recorder record.EventRecorder,
	namespace string,
	isTest bool) *ValidationReconciler {

	return &ValidationReconciler{
		XJoinSynchronizerReconciler: *NewXJoinReconciler(client, scheme, log, recorder, namespace, isTest),
		CheckResourceDeviation:      checkResourceDeviation,
	}
}
