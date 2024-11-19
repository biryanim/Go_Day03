package db

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"go_day03/pkg/types"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var schema = `{
	  "settings": {
	    "number_of_shards": 1,
		"max_result_window": 20000
	  },
	  "mappings": {
        "properties": {
   		  "name": {
		    "type":  "text"
		  },
          "address": {
		    "type":  "text"
		  },
		  "phone": {
			"type":  "text"
		  },
		  "location": {
		    "type": "geo_point"
		  }
		}
	  }
	}`

type Elastic struct {
	Client *elasticsearch.Client
}

func NewElastic() (*Elastic, error) {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		return nil, err
	}
	return &Elastic{es}, nil
}

func (el *Elastic) LoadData() {
	var (
		res       *esapi.Response
		indexName = "places"
		es        = el.Client
	)

	res, err := es.Indices.Delete([]string{indexName}, es.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil || res.IsError() {
		log.Fatalf("Cannot delete index: %s", err)
	}
	defer res.Body.Close()
	res, err = es.Indices.Create(indexName, es.Indices.Create.WithBody(strings.NewReader(schema)))
	if err != nil {
		log.Fatal("Cannot create index", err)
	}
	if res.IsError() {
		log.Fatal("Cannot create index ", res)
	}
	defer res.Body.Close()
	err = ReadCsvFile("../materials/data.csv", es)
	if err != nil {
		log.Fatal(err)
	}

}

func ReadCsvFile(filepath string, es *elasticsearch.Client) error {
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "places",
		Client:        es,
		NumWorkers:    8,
		FlushBytes:    10000,
		FlushInterval: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	csvReader.Comma = '\t'
	_, err = csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}
	for {
		line, err := csvReader.Read()
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		id, err1 := strconv.Atoi(line[0])
		lat, err2 := strconv.ParseFloat(line[5], 64)
		lon, err3 := strconv.ParseFloat(line[4], 64)
		if err1 != nil || err2 != nil || err3 != nil {
			return errors.New("Cannot parse line")
		}
		place := &types.Place{
			ID:      id + 1,
			Name:    line[1],
			Address: line[2],
			Phone:   line[3],
			Location: struct {
				Lat float64 `json:"lat"`
				Lon float64 `json:"lon"`
			}{Lat: lat, Lon: lon},
		}
		data, err := json.Marshal(place)
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: strconv.Itoa(place.ID),
				Body:       bytes.NewReader(data),
			},
		)

	}
	if err := bi.Close(context.Background()); err != nil {
		return err
	}
	return nil
}

func (el *Elastic) GetPlaces(limit int, offset int) ([]types.Place, int, error) {
	query := map[string]interface{}{
		"size": limit,
		"from": offset,
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, 0, err
	}

	req := esapi.SearchRequest{
		Index:          []string{"places"},
		Body:           strings.NewReader(string(queryJSON)),
		TrackTotalHits: true,
		Pretty:         true,
	}
	res, err := req.Do(context.Background(), el.Client)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return nil, 0, fmt.Errorf("Elasticsearch search request failed: %s", res.String())
	}
	var body map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, 0, err
	}
	hits := body["hits"].(map[string]interface{})["hits"].([]interface{})
	places := make([]types.Place, 0)
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		placeBytes, err := json.Marshal(source)
		if err != nil {
			continue
		}
		var place types.Place
		if err := json.Unmarshal(placeBytes, &place); err != nil {
			continue
		}
		places = append(places, place)
	}
	total := int(body["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	return places, total, nil
}

func (el *Elastic) GetClosestPlace(lat float64, lon float64) ([]types.Place, error) {
	query := fmt.Sprintf(`{
	"size": 3,
	"sort": [
    {
      "_geo_distance": {
        "location": {
          "lat": %v,
          "lon": %v
        },
        "order": "asc",
        "unit": "km",
        "mode": "min",
        "distance_type": "arc",
        "ignore_unmapped": true
      }
    }
]
}`, lat, lon)
	req := esapi.SearchRequest{
		Index:          []string{"places"},
		Body:           strings.NewReader(query),
		TrackTotalHits: true,
		Pretty:         true,
	}
	res, err := req.Do(context.Background(), el.Client)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch search request failed: %s", res.String())
	}
	var body map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	hits := body["hits"].(map[string]interface{})["hits"].([]interface{})
	places := make([]types.Place, 0)
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"]
		placeBytes, err := json.Marshal(source)
		if err != nil {
			continue
		}
		var place types.Place
		if err := json.Unmarshal(placeBytes, &place); err != nil {
			continue
		}
		places = append(places, place)
	}
	return places, nil
}
