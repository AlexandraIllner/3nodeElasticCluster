import pandas as pd
from elasticsearch import Elasticsearch

ELASTIC_PW1 = "yWZ2wRvSnWY5mkh9koWA"

es_cluster1 = Elasticsearch(
    hosts="https://localhost:9200",
    basic_auth=("elastic", ELASTIC_PW1),
    ca_certs="http_ca.crt"
)

if es_cluster1.ping():
    print("Connected with Elasticsearch, Info: ", es_cluster1.info().body)
else:
    print(f"Connection Failed")

df = (
    pd.read_csv("wiki_movie_plots_deduped.csv")
    .dropna()
    .sample(1001, random_state=92)
    .reset_index(drop=True)
)

# print(df)

mappings = {
    "properties": {
        "title": {"type": "text", "analyzer": "english"},
        "ethnicity": {"type": "text", "analyzer": "standard"},
        "director": {"type": "text", "analyzer": "standard"},
        "cast": {"type": "text", "analyzer": "standard"},
        "genre": {"type": "text", "analyzer": "standard"},
        "plot": {"type": "text", "analyzer": "english"},
        "year": {"type": "integer"},
        "wiki_page": {"type": "keyword"}
    }
}

# für Neustarts Index löschen
es_cluster1.indices.delete(index="movies1001")

# neuen Index anlegen
es_cluster1.indices.create(index="movies1001", mappings=mappings)


def fill_movies1001():
    for i, row in df.iterrows():
        doc = {
            "title": row["Title"],
            "ethnicity": row["Origin/Ethnicity"],
            "director": row["Director"],
            "cast": row["Cast"],
            "genre": row["Genre"],
            "plot": row["Plot"],
            "year": row["Release Year"],
            "wiki_page": row["Wiki Page"]
        }

        es_cluster1.index(index="movies1001", id=i, document=doc)


fill_movies1001()

response = es_cluster1.search(index="movies1001", query={
    "fuzzy": {
        "plot": {
            "value": "woman"
        }
    }
})
print("Suche: ", response, '\n', '\n', '\n')

health = es_cluster1.cluster.health()

print("Cluster Health: ", health)
