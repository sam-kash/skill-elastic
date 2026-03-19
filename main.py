from fastapi import FastAPI
import psycopg2
from elasticsearch import Elasticsearch

app = FastAPI()

conn = psycopg2.connect(
    database="skills_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

es = Elasticsearch("http://localhost:9200")

def create_index():
    try:
        es.indices.create(
            index="skills",
            body={
                "mappings": {
                    "properties": {
                        "name": {"type": "text"}
                    }
                }
            }
        )
        print("Index created")
    except Exception as e:
        print("Index already exists or error:", e)

def index_skills():
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM skills")
    rows = cur.fetchall()

    for row in rows:
        es.index(
            index="skills",
            id=row[0],
            document={"name": row[1]}
        )

create_index()
index_skills()

@app.get("/autocomplete")
def autocomplete(q: str):
    res = es.search(
        index="skills",
        body={
            "query": {
                "match": {
                    "name": q
                }
            }
        }
    )

    return [hit["_source"]["name"] for hit in res["hits"]["hits"]]