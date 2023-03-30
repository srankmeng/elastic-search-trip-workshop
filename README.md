# POC search material

## Setup Elasticsearch and Kibana with [Docker compose](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html)
```
$ docker compose up -d
$ docker-compose up --build --force-recreate
$ docker compose ps
$ docker compose log --follow
```

## Example of data
```
POST materials/_doc
{
	"material_id": 1,
	"spec_no": "M1111A222",
	"spec_name": "material one",
	"spec_shortname": "material one",
	"created_date": "2023/02/28",
	"status": "Yes",
	"cross_reference": ["x111", "y222"]
}
```

## Create a custom tokenizer and analyzer
```
PUT materials
{
  "settings": {
    "index": {
      "number_of_shards": 4,
      "number_of_replicas": 1,
      "max_ngram_diff": "5",
      "analysis": {
        "analyzer": {
          "trigrams": {
            "tokenizer": "trigram_tokenizer",
            "filter": [
              "lowercase"
            ]
          }
        },
        "tokenizer": {
          "trigram_tokenizer": {
            "type": "ngram",
            "min_gram": 2,
            "max_gram": 5,
            "token_chars": []
          }
        }
      }
    }
  }
}
```

Check setting and mapping
```
GET materials/_settings

GET materials/_mapping
```

Testing with ngram
```
GET materials/_analyze
{
  "text": "my data 123",
  "analyzer": "trigrams"
}
```

## Custom mapping for materials
```
PUT materials/_mapping
{
  "properties": {
    "spec_no": {
      "analyzer": "trigrams",
      "type": "text",
      "fields": {
        "raw": {
          "type": "keyword"
        }
      }
    }
  }
}
```

## Search data with [Ngram](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-ngram-tokenizer.html)
```
GET materials/_search
{
  "query": {
    "bool": {
      "should": [
        {
          "term": {
            "spec_no": {
              "value": "m1"
            }
          }
        }
      ]
    }
  }
}
```
## Import data csv file with [Python]

**Note:** Change host url, index name and setting/mapping before run command 
Use command inside directory indexer
```
$ cd indexer
```
and use command for run script import data with python
```
$ python3 -m venv env/local && source env/local/bin/activate
$ pip3 install -r requirements.txt
$ python3 indexer.py
```

Waiting a moment for upload data and enjoy :beers: :v:
