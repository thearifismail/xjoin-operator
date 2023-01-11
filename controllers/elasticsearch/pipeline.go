package elasticsearch

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
)

func (es *ElasticSearch) ESSynchronizerName(synchronizerVersion string) string {
	return es.resourceNamePrefix + "." + synchronizerVersion
}

func (es *ElasticSearch) ESSynchronizerExists(synchronizerVersion string) (bool, error) {
	req := esapi.IngestGetSynchronizerRequest{
		DocumentID: es.ESSynchronizerName(synchronizerVersion),
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, es.Client)
	if err != nil {
		return false, err
	}

	resCode, _, err := parseResponse(res)
	if resCode == 404 {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (es *ElasticSearch) GetESSynchronizer(synchronizerVersion string) (map[string]interface{}, error) {
	req := esapi.IngestGetSynchronizerRequest{
		DocumentID: es.ESSynchronizerName(synchronizerVersion),
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, es.Client)
	if err != nil {
		return nil, err
	}
	resCode, body, err := parseResponse(res)

	if resCode != 200 {
		return nil, errors.New(fmt.Sprintf(
			"invalid response from ElasticSearch. StatusCode: %s, Body: %s",
			strconv.Itoa(res.StatusCode), body))
	}

	return body, err
}

func (es *ElasticSearch) CreateESSynchronizer(synchronizerVersion string) error {
	tmpl, err := template.New("synchronizerTemplate").Parse(es.synchronizerTemplate)
	if err != nil {
		return err
	}

	var synchronizerTemplateBuffer bytes.Buffer
	err = tmpl.Execute(&synchronizerTemplateBuffer, es.parametersMap)
	if err != nil {
		return err
	}
	synchronizerTemplateParsed := synchronizerTemplateBuffer.String()
	synchronizerTemplateParsed = strings.ReplaceAll(synchronizerTemplateParsed, "\n", "")
	synchronizerTemplateParsed = strings.ReplaceAll(synchronizerTemplateParsed, "\t", "")

	res, err := es.Client.Ingest.PutSynchronizer(
		es.ESSynchronizerName(synchronizerVersion), strings.NewReader(synchronizerTemplateParsed))
	if err != nil {
		return err
	}

	statusCode, _, err := parseResponse(res)
	if err != nil || statusCode != 200 {
		return err
	}

	return nil
}

func (es *ElasticSearch) ListESSynchronizers(synchronizerIds ...string) ([]string, error) {
	req := esapi.IngestGetSynchronizerRequest{}

	if synchronizerIds != nil {
		req.DocumentID = "*" + synchronizerIds[0] + "*"
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, es.Client)
	if err != nil {
		return nil, err
	}
	resCode, body, err := parseResponse(res)
	var esSynchronizers []string

	if resCode == 404 {
		return esSynchronizers, nil
	} else if resCode != 200 {
		return nil, errors.New(fmt.Sprintf(
			"Unable to list es synchronizers. StatusCode: %s, Body: %s",
			strconv.Itoa(res.StatusCode), body))
	}

	for esSynchronizerName, _ := range body {
		esSynchronizers = append(esSynchronizers, esSynchronizerName)
	}

	return esSynchronizers, err
}

func (es *ElasticSearch) DeleteESSynchronizerByVersion(version string) error {
	return es.DeleteESSynchronizerByFullName(es.ESSynchronizerName(version))
}

func (es *ElasticSearch) DeleteESSynchronizerByFullName(esSynchronizer string) error {
	if esSynchronizer == "" {
		return nil
	}

	_, err := es.Client.Ingest.DeleteSynchronizer(esSynchronizer)
	if err != nil {
		return err
	}
	return nil
}
