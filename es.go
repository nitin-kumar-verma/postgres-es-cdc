package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/esapi"
)

func IndexExists(indexName string) bool {
	req := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Error checking index existence: %v", err)
	}
	defer res.Body.Close()
	//res.StatusCode == 404 implies index doesn;t exist

	return res.StatusCode == 200
}

func CreateIndex(indexName string) {

	req := esapi.IndicesCreateRequest{
		Index: indexName,
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Error creating index: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		handleIndexError(res)
	}

	//fmt.Printf("Index %s created successfully\n", indexName)
}

func UpdateDocument(indexName string, payload map[string]interface{}) {

	updateByQueryBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"id": payload["id"],
			},
		},
		"script": map[string]interface{}{
			"source": fmt.Sprintf("ctx._source.putAll(params.data)"),
			"lang":   "painless",
			"params": map[string]interface{}{
				"data": payload,
			},
		},
	}

	// Marshal the body to JSON
	jsonBody, err := json.Marshal(updateByQueryBody)
	if err != nil {
		log.Fatalf("Error marshalling insert request: %v", err)
	}

	// Create the update by query request
	updateByQueryRequest := esapi.UpdateByQueryRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(string(jsonBody)),
	}

	// Perform the update by query request
	res, err := updateByQueryRequest.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Error updating document: %s", res.Status())
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error inserting document: %s", res.Status())
	}
}

func InsertDocument(indexName string, payload map[string]interface{}) {

	documentJSON, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Error marshalling insert request: %v", err)
	}

	req := esapi.IndexRequest{
		Index: indexName,
		Body:  strings.NewReader(string(documentJSON)),
	}

	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Error inserting document: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error inserting document: %s", res.Status())
	}

}

func DeleteDocument(indexName string, payload map[string]interface{}) {

	deleteByQueryBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"id": payload["id"],
			},
		},
	}

	// Marshal the body to JSON
	jsonBody, err := json.Marshal(deleteByQueryBody)
	if err != nil {
		log.Fatalf("Error marshalling insert request: %v", err)
	}

	// Create the delete by query request
	deleteByQueryRequest := esapi.DeleteByQueryRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(string(jsonBody)),
	}

	// Perform the delete by query request
	response, err := deleteByQueryRequest.Do(context.Background(), esClient)
	if err != nil {
		log.Fatalf("Error deleting document: %v", err)
	}
	defer response.Body.Close()

	if response.IsError() {
		log.Fatalf("Error deleting document: %v", err)
	}
}

func handleIndexError(res *esapi.Response) {
	var errorResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&errorResponse); err != nil {
		log.Fatalf("Error parsing the error response: %v", err)
	}
	log.Fatalf("Error: %s", errorResponse["error"].(map[string]interface{})["reason"])
}
