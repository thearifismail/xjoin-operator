package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"text/template"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/go-errors/errors"
)

type GenericElasticsearch struct {
	Parameters map[string]interface{}
	Client     *elasticsearch.Client
	Context    context.Context
}

type GenericElasticSearchParameters struct {
	Url        string
	Username   string
	Password   string
	Parameters map[string]interface{}
	Context    context.Context
}

func NewGenericElasticsearch(params GenericElasticSearchParameters) (*GenericElasticsearch, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{params.Url},
		Username:  params.Username,
		Password:  params.Password,
		//Transport: &http.Transport{
		//	TLSClientConfig: &tls.Config{
		//		InsecureSkipVerify: false,
		//	},
		//},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	es := GenericElasticsearch{
		Parameters: params.Parameters,
		Client:     client,
		Context:    params.Context,
	}

	return &es, nil
}

func (es GenericElasticsearch) IndexExists(indexName string) (bool, error) {
	res, err := es.Client.Indices.Exists([]string{indexName})
	if err != nil {
		return false, errors.Wrap(err, 0)
	}

	responseCode, _, err := parseResponse(res)
	if err != nil && responseCode != 404 {
		return false, errors.Wrap(err, 0)
	} else if responseCode == 404 {
		return false, nil
	}

	return true, nil
}

func (es *GenericElasticsearch) DeleteIndexByFullName(index string) error {
	if index == "" {
		return nil
	}

	res, err := es.Client.Indices.Delete([]string{index})
	if err != nil {
		return errors.Wrap(err, 0)
	}

	responseCode, _, err := parseResponse(res)
	if err != nil && responseCode != 404 {
		return errors.Wrap(err, 0)
	}

	return nil
}

func (es GenericElasticsearch) ListIndicesForPrefix(prefix string) ([]string, error) {
	req := esapi.CatIndicesRequest{
		Format: "JSON",
		Index:  []string{prefix + ".*"},
		H:      []string{"index"},
	}
	res, err := req.Do(es.Context, es.Client)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	defer res.Body.Close()

	byteValue, _ := ioutil.ReadAll(res.Body)

	var indicesJSON []map[string]string
	err = json.Unmarshal(byteValue, &indicesJSON)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	var indices []string
	for _, index := range indicesJSON {
		indices = append(indices, index["index"])
	}

	return indices, nil
}

func (es GenericElasticsearch) CreateSynchronizer(name string, synchronizer string) (err error) {
	res, err := es.Client.Ingest.PutSynchronizer(
		name, strings.NewReader(synchronizer))
	if err != nil {
		return errors.Wrap(err, 0)
	}

	statusCode, _, err := parseResponse(res)
	if err != nil {
		return errors.Wrap(err, 0)
	} else if statusCode != 200 {
		return errors.Wrap(errors.New("Invalid status code when creating Elasticsearch Synchronizer: "+strconv.Itoa(statusCode)), 0)
	}
	return
}

func (es GenericElasticsearch) DeleteSynchronizer(name string) (err error) {
	_, err = es.Client.Ingest.DeleteSynchronizer(name)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return
}

func (es GenericElasticsearch) SynchronizerExists(name string) (exists bool, err error) {
	req := esapi.IngestGetSynchronizerRequest{
		DocumentID: name,
	}

	res, err := req.Do(es.Context, es.Client)
	if err != nil {
		return false, errors.Wrap(err, 0)
	}

	resCode, _, err := parseResponse(res)
	if resCode == 404 {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, 0)
	} else {
		return true, nil
	}
}

func (es GenericElasticsearch) ListSynchronizersForPrefix(prefix string) (esSynchronizers []string, err error) {
	req := esapi.IngestGetSynchronizerRequest{
		DocumentID: prefix + "*",
	}

	res, err := req.Do(es.Context, es.Client)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}
	resCode, body, err := parseResponse(res)

	if resCode == 404 {
		return esSynchronizers, nil
	} else if resCode != 200 {
		return nil, errors.Wrap(errors.New(fmt.Sprintf(
			"Unable to list es synchronizers. StatusCode: %s, Body: %s",
			strconv.Itoa(res.StatusCode), body)), 0)
	} else if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	for esSynchronizerName, _ := range body {
		esSynchronizers = append(esSynchronizers, esSynchronizerName)
	}

	return
}

func (es GenericElasticsearch) CreateIndex(
	indexName string, indexTemplate string, properties string, withSynchronizer bool) error {

	tmpl, err := template.New("indexTemplate").Parse(indexTemplate)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	params := es.Parameters
	params["ElasticSearchIndex"] = indexName
	params["ElasticSearchProperties"] = properties

	if withSynchronizer {
		params["ElasticSearchSynchronizer"] = indexName
	} else {
		params["ElasticSearchSynchronizer"] = "_none"
	}

	var indexTemplateBuffer bytes.Buffer
	err = tmpl.Execute(&indexTemplateBuffer, params)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	indexTemplateParsed := indexTemplateBuffer.String()
	indexTemplateParsed = strings.ReplaceAll(indexTemplateParsed, "\n", "")
	indexTemplateParsed = strings.ReplaceAll(indexTemplateParsed, "\t", "")

	req := &esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(indexTemplateParsed),
	}

	res, err := req.Do(es.Context, es.Client)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	_, _, err = parseResponse(res)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}
