curl -H "Content-Type: application/json" -H "Authorization: Basic ZWxhc3RpYzoxMjM0NTY=" -XPUT "http://172.16.1.119:9200/person_index" -d'
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
                },
                "date_time": {
                    "type": "date"
                }

            }
        }
    }
}'

curl -H "Content-Type: application/json" -H "Authorization: Basic ZWxhc3RpYzoxMjM0NTY=" -XPUT "http://172.16.1.119:9200/my_join_index" -d'
{
    "mappings": {
        "data": {
            "properties": {
                "join_field": {
                    "type": "join",
                    "relations": {
                        "father": "son"
                    }
                },
                "name": {
                    "type": "keyword",
                    "index": "true"
                }
            }
        }
    }
}
'

根据父找子、子找父
https://blog.csdn.net/laoyang360/article/details/79774481