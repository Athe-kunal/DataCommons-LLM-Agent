import yaml
import json
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
from dotenv import load_dotenv,find_dotenv
from chromadb.utils.batch_utils import create_batches
import os

with open("config.yaml") as stream:
    try:
        config_params = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

def build_dcids_database():
    with open(
        config_params["DCID_SAVE_PATH"]["JSON_FILE_PATH"],
        "r",
        encoding="utf-8",
    ) as json_file:
        all_dcids = json.load(json_file)
    docs = []
    metadata = []
    for dcid_data in all_dcids:
        docs.append(dcid_data['stats_desc'])
        metadata.append({"link":dcid_data['stats_link'],"dcid":dcid_data['stats_dcid']})
    
    database_path = config_params["VECTORDB"]["BASE_DATABASE_PATH"]
    collection_name = config_params["VECTORDB"]["COLLECTION_NAME"]
    load_dotenv(find_dotenv(), override=True)
    emb_fn = embedding_functions.OpenAIEmbeddingFunction(
        api_key=os.environ['OPENAI_API_KEY'], model_name="text-embedding-3-small"
    )

    client = chromadb.PersistentClient(path=database_path)
    sklearn_collection = client.get_or_create_collection(
        name=collection_name, embedding_function=emb_fn
    )

    ids = [f"id{i}" for i in range(len(docs))]
    batches = create_batches(
        api=client, ids=ids, documents=docs, metadatas=metadata
    )
    for batch in batches:
        sklearn_collection.add(ids=batch[0], documents=batch[3], metadatas=batch[2])
    return sklearn_collection

def load_database():
    load_dotenv(find_dotenv(), override=True)
    database_path = config_params["VECTORDB"]["BASE_DATABASE_PATH"]
    collection_name = config_params["VECTORDB"]["COLLECTION_NAME"]
    emb_fn = embedding_functions.OpenAIEmbeddingFunction(
        api_key=os.environ['OPENAI_API_KEY'], model_name=config_params["VECTORDB"]["EMBEDDING_MODEL_NAME"]
    )
    client = chromadb.PersistentClient(path=database_path)
    dcid_collection = client.get_collection(
        name=collection_name, embedding_function=emb_fn
    )
    return dcid_collection


