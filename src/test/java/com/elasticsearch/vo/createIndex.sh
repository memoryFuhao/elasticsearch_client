curl -H "Content-Type: application/json" -H "Authorization: Basic ZWxhc3RpYzoxMjM0NTY=" -XPUT "http://$IP:9200/archive_title_result" -d'
{
    "mappings": {
        "data": {
            "properties": {
                "name": {
                    "type": "keyword"
                },
                "id": {
                    "type": "keyword"
                },
                "age": {
                    "type": "integer"
                },
                "hobby": {
                    "type": "keyword"
                }
            }
        }
    }
}'