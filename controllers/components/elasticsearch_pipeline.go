package components

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-errors/errors"
	"github.com/redhatinsights/xjoin-operator/controllers/elasticsearch"
)

type ElasticsearchSynchronizer struct {
	name                 string
	version              string
	JsonFields           []string
	GenericElasticsearch elasticsearch.GenericElasticsearch
}

func (es *ElasticsearchSynchronizer) SetName(name string) {
	es.name = strings.ToLower(name)
}

func (es *ElasticsearchSynchronizer) SetVersion(version string) {
	es.version = version
}

func (es ElasticsearchSynchronizer) Name() string {
	return es.name + "." + es.version
}

func (es ElasticsearchSynchronizer) Create() (err error) {
	synchronizer, err := es.jsonFieldsToESSynchronizer()
	if err != nil {
		return errors.Wrap(err, 0)
	}
	err = es.GenericElasticsearch.CreateSynchronizer(es.Name(), synchronizer)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (es ElasticsearchSynchronizer) Delete() (err error) {
	err = es.GenericElasticsearch.DeleteSynchronizer(es.Name())
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (es *ElasticsearchSynchronizer) CheckDeviation() (err error) {
	return
}

func (es ElasticsearchSynchronizer) Exists() (exists bool, err error) {
	exists, err = es.GenericElasticsearch.SynchronizerExists(es.Name())
	if err != nil {
		return false, errors.Wrap(err, 0)
	}
	return
}

func (es ElasticsearchSynchronizer) ListInstalledVersions() (versions []string, err error) {
	synchronizers, err := es.GenericElasticsearch.ListSynchronizersForPrefix(es.name)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	for _, synchronizer := range synchronizers {
		versions = append(versions, strings.Split(synchronizer, es.name+".")[1])
	}
	return
}

func (es ElasticsearchSynchronizer) jsonFieldsToESSynchronizer() (synchronizer string, err error) {
	var synchronizerObj elasticsearch.Synchronizer
	synchronizerObj.Description = "test"
	for _, jsonField := range es.JsonFields {
		var processor elasticsearch.SynchronizerProcessor
		processor.Json.Field = jsonField
		processor.Json.If = fmt.Sprintf("ctx.%s != null", jsonField)
		synchronizerObj.Processors = append(synchronizerObj.Processors, processor)
	}

	synchronizerJson, err := json.Marshal(synchronizerObj)
	if err != nil {
		return synchronizer, errors.Wrap(err, 0)
	}
	return string(synchronizerJson), nil
}
