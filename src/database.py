import yaml
import json
import chromadb
import chromadb.utils.embedding_functions as embedding_functions
from dotenv import load_dotenv,find_dotenv
from chromadb.utils.batch_utils import create_batches
import os
import ast

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
    for stat_files in os.listdir("data/STATS"):
        stat_file_name = ".".join(stat_files.split("_"))
        with open(os.path.join("data/STATS",stat_files), "r") as f:
            content = json.load(f)
        for stat in content:
            # docs.append(Document(page_content=stat['node_name'],metadata={'dcid': stat['node_dcid'],'link': stat['node_link'],'data_source':stat_file_name}))
            stat_desc = stat['node_name']
            stat_desc = stat_desc.replace("\u2026 ","")
            stat_desc = stat_desc.replace("…","")
            if stat['node_name'] == "":
                docs.append(stat['node_dcid'])
            else:
                docs.append(stat['node_name'].strip())
            metadata.append({"link":stat['node_link'],"dcid":stat['node_dcid']})
    with open('doc.txt', "w") as file:
        # Iterate over the list of strings
        for string in docs:
            # Write each string followed by a newline character
            file.write(string + "\n")
    database_path = config_params["VECTORDB"]["BASE_DATABASE_PATH"]
    collection_name = config_params["VECTORDB"]["COLLECTION_NAME"]
    load_dotenv(find_dotenv(), override=True)
    emb_fn = embedding_functions.OpenAIEmbeddingFunction(
        api_key=os.environ['OPENAI_API_KEY'], model_name="text-embedding-3-small"
    )

    client = chromadb.PersistentClient(path=database_path)
    dcid_collection = client.get_or_create_collection(
        name=collection_name, embedding_function=emb_fn
    )

    ids = [f"id{i}" for i in range(len(docs))]
    batches = create_batches(
        api=client, ids=ids, documents=docs, metadatas=metadata
    )
    for batch in batches:
        dcid_collection.add(ids=batch[0], documents=batch[3], metadatas=batch[2])
    return dcid_collection

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


