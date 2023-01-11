package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
	xjoin "github.com/thearifismail/xjoin-operator/api/v1alpha1"
	logger "github.com/thearifismail/xjoin-operator/controllers/log"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var log = logger.NewLogger("k8s")

func FetchXJoinSynchronizer(c client.Client, namespacedName types.NamespacedName, ctx context.Context) (*xjoin.XJoinSynchronizer, error) {
	instance := &xjoin.XJoinSynchronizer{}
	err := c.Get(ctx, namespacedName, instance)
	return instance, err
}

func FetchXJoinDataSourceSynchronizer(c client.Client, namespacedName types.NamespacedName, ctx context.Context) (*xjoin.XJoinDataSourceSynchronizer, error) {
	instance := &xjoin.XJoinDataSourceSynchronizer{}
	err := c.Get(ctx, namespacedName, instance)
	return instance, err
}

func FetchXJoinDataSource(c client.Client, namespacedName types.NamespacedName, ctx context.Context) (*xjoin.XJoinDataSource, error) {
	instance := &xjoin.XJoinDataSource{}
	err := c.Get(ctx, namespacedName, instance)
	return instance, err
}

func FetchXJoinIndex(c client.Client, namespacedName types.NamespacedName, ctx context.Context) (*xjoin.XJoinIndex, error) {
	instance := &xjoin.XJoinIndex{}
	err := c.Get(ctx, namespacedName, instance)
	return instance, err
}

func FetchXJoinIndexes(c client.Client, ctx context.Context) (*xjoin.XJoinIndexList, error) {
	list := &xjoin.XJoinIndexList{}
	err := c.List(ctx, list)
	return list, err
}

func FetchXJoinIndexSynchronizer(c client.Client, namespacedName types.NamespacedName, ctx context.Context) (*xjoin.XJoinIndexSynchronizer, error) {
	instance := &xjoin.XJoinIndexSynchronizer{}
	err := c.Get(ctx, namespacedName, instance)
	return instance, err
}

func FetchXJoinIndexValidator(c client.Client, namespacedName types.NamespacedName, ctx context.Context) (*xjoin.XJoinIndexValidator, error) {
	instance := &xjoin.XJoinIndexValidator{}
	err := c.Get(ctx, namespacedName, instance)
	return instance, err
}

func FetchXJoinSynchronizers(c client.Client, ctx context.Context) (*xjoin.XJoinSynchronizerList, error) {
	list := &xjoin.XJoinSynchronizerList{}
	err := c.List(ctx, list)
	return list, err
}

func FetchConfigMap(c client.Client, namespace string, name string, ctx context.Context) (*corev1.ConfigMap, error) {
	configMap := &corev1.ConfigMap{}
	err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, configMap)
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}
	return configMap, nil
}

func SecretHash(secret *corev1.Secret) (string, error) {
	if secret == nil {
		return "-1", nil
	}

	jsonVal, err := json.Marshal(secret.Data)

	if err != nil {
		return "-2", nil
	}

	algorithm := fnv.New32a()
	_, err = algorithm.Write(jsonVal)
	return fmt.Sprint(algorithm.Sum32()), err
}

func SpecHash(spec interface{}) (string, error) {
	jsonVal, err := json.Marshal(spec)
	if err != nil {
		return "", errors.Wrap(err, 0)
	}

	algorithm := fnv.New32a()
	_, err = algorithm.Write(jsonVal)
	return fmt.Sprint(algorithm.Sum32()), err
}

func ConfigMapHash(cm *corev1.ConfigMap, ignoredKeys ...string) (string, error) {
	if cm == nil {
		return "-1", nil
	}

	values := utils.Omit(cm.Data, ignoredKeys...)

	jsonVal, err := json.Marshal(values)

	if err != nil {
		return "-2", nil
	}

	algorithm := fnv.New32a()
	_, err = algorithm.Write(jsonVal)
	return fmt.Sprint(algorithm.Sum32()), err
}

func FetchSecret(c client.Client, namespace string, name string, ctx context.Context) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret)

	if statusError, isStatus := err.(*k8errors.StatusError); isStatus && statusError.Status().Reason == metav1.StatusReasonNotFound {
		log.Info("Secret not found.", "namespace", namespace, "name", name)
		return nil, nil
	}

	return secret, err
}
