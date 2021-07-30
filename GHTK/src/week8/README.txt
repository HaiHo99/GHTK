Bài 1: GET /dantri_haiht34/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "time": {
              "gte":  1356998400,
              "lt": 1388534400
            }
          }
        },
        {
          "multi_match": {
            "query": "an toàn, đường bộ, đường sắt",
            "fields": ["content"]
          }
        }
      ]
    }
  }
}

Bài 2: PUT dantri_haiht34
{
  "mappings": {
    "properties": {
      "title": {
      "type": "keyword"
      }
    }
  }
}
GET dantri_haiht34/_search
{
  "query": {
      "bool": {
        "must": [
        {
          "prefix": {
            "title.keyword": "Hà Nội"
            }
        }
      ]
      , "must_not": [
        {
          "match_phrase": {
            "description.keyword": "Hà Nội"
          }
        }
      ]
    }
  },
  "sort" : {
    "time" : { "order" : "desc" }
  }
}
Bài 3:

main.py để extract title từ các file data.json
ElasticsService.java để search suggestion.
