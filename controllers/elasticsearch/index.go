package elasticsearch

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/redhatinsights/xjoin-go-lib/pkg/utils"
)

func (es *ElasticSearch) IndexExists(indexName string) (bool, error) {
	res, err := es.Client.Indices.Exists([]string{indexName})
	if err != nil {
		return false, err
	}

	responseCode, _, err := parseResponse(res)
	if err != nil && responseCode != 404 {
		return false, err
	} else if responseCode == 404 {
		return false, nil
	}

	return true, nil
}

func (es *ElasticSearch) CreateIndex(synchronizerVersion string) error {
	tmpl, err := template.New("indexTemplate").Parse(es.indexTemplate)
	if err != nil {
		return err
	}

	params := es.parametersMap
	params["ElasticSearchIndex"] = es.ESIndexName(synchronizerVersion)
	params["ElasticSearchSynchronizer"] = es.ESSynchronizerName(synchronizerVersion)

	var indexTemplateBuffer bytes.Buffer
	err = tmpl.Execute(&indexTemplateBuffer, es.parametersMap)
	if err != nil {
		return err
	}
	indexTemplateParsed := indexTemplateBuffer.String()
	indexTemplateParsed = strings.ReplaceAll(indexTemplateParsed, "\n", "")
	indexTemplateParsed = strings.ReplaceAll(indexTemplateParsed, "\t", "")

	req := &esapi.IndicesCreateRequest{
		Index: es.ESIndexName(synchronizerVersion),
		Body:  strings.NewReader(indexTemplateParsed),
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, es.Client)

	if err != nil {
		return err
	}

	_, _, err = parseResponse(res)
	return err
}

func (es *ElasticSearch) DeleteIndexByFullName(index string) error {
	if index == "" {
		return nil
	}

	res, err := es.Client.Indices.Delete([]string{index})
	if err != nil {
		return err
	}

	responseCode, _, err := parseResponse(res)
	if err != nil && responseCode != 404 {
		return err
	}

	return nil
}

func (es *ElasticSearch) DeleteIndex(version string) error {
	return es.DeleteIndexByFullName(es.ESIndexName(version))
}

func (es *ElasticSearch) ListIndices() ([]string, error) {
	req := esapi.CatIndicesRequest{
		Format: "JSON",
		Index:  []string{es.resourceNamePrefix + ".*"},
		H:      []string{"index"},
	}
	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, es.Client)
	if err != nil {
		return nil, err
	}

	byteValue, _ := ioutil.ReadAll(res.Body)

	var indicesJSON []map[string]string
	err = json.Unmarshal(byteValue, &indicesJSON)
	if err != nil {
		return nil, err
	}

	var indices []string
	for _, index := range indicesJSON {
		indices = append(indices, index["index"])
	}

	defer res.Body.Close()
	return indices, nil
}

func (es *ElasticSearch) CountIndex(index string) (int, error) {
	req := esapi.CountRequest{
		Index: []string{index},
	}

	ctx, cancel := utils.DefaultContext()
	defer cancel()
	res, err := req.Do(ctx, es.Client)
	if err != nil {
		return -1, err
	}

	var countIDsResponse CountIDsResponse
	byteValue, _ := ioutil.ReadAll(res.Body)
	err = json.Unmarshal(byteValue, &countIDsResponse)
	if err != nil {
		return -1, err
	}

	return countIDsResponse.Count, nil
}

func (es *ElasticSearch) ESIndexName(synchronizerVersion string) string {
	return ESIndexName(es.resourceNamePrefix, synchronizerVersion)
}

func ESIndexName(resourceNamePrefix string, synchronizerVersion string) string {
	return resourceNamePrefix + "." + synchronizerVersion
}
