from elasticsearch import Elasticsearch
from typing import List


class LectureMaterialSearcher:
    def __init__(self, es_host: str = "localhost", es_port: int = 9200,
                 es_user: str = "elastic", es_password: str = "secret"):
        self.es = Elasticsearch(
            hosts=[f"http://{es_host}:{es_port}"],
            basic_auth=(es_user, es_password),
            verify_certs=False
        )

    def search_by_course_and_session_type(self, query: str, session_type_id: str) -> List[int]:
        response = self.es.search(
            index="lecture_sessions",
            query={
                "bool": {
                    "must": [  # Обязательные условия
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["lecture_name^3", "course_name^2", "content", "keywords"],
                                "type": "best_fields",
                                "fuzziness": "AUTO"
                            }
                        }
                    ],
                    "filter": [
                        {
                            "term": {
                                "session_type_id": session_type_id
                            }
                        }
                    ]
                }
            }
        )
        return [hit['_source']['session_id'] for hit in response['hits']['hits']]
