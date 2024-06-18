import dspy
from langchain_community.retrievers import BM25Retriever
from dotenv import load_dotenv,find_dotenv
import concurrent.futures
import datacommons_pandas as dc
from typing import Annotated, List
import chromadb.utils.embedding_functions as embedding_functions
import chromadb
import os
from langchain.schema import Document
from src.get_place_dcids import place_dcid

load_dotenv(find_dotenv(),override=True)
llm = dspy.OpenAI(model="gpt-3.5-turbo")
dspy.settings.configure(lm=llm)
"""Returns the places that the question is talking about separated by semicolon (;) and also only the noun keywords relevant to the question in a list
    Make sure that you are only outputing the noun keywords and not other things"""
class PlaceKeywordSignature(dspy.Signature):
    """Returns the places that the question is talking about separated by semicolon (;)"""
    question = dspy.InputField(prefix="Question: ",desc="Question asked by the user")
    places = dspy.OutputField(prefix="Places: ",desc="places like countries, states, towns, etc mentioned in the question separated by semicolon (;)")
    # keywords = dspy.OutputField(prefix="Keywords: ",desc="noun keywords relevant to the question in a list. DON'T include the place names and be precise")

class SelectDCIDSignature(dspy.Signature):
    """Based on the dcid and their descriptions, select the dcid(s) that are most relevant to the question. Return the relevant dcids separated by semicolon (;)
    Don't output anything else, just output the relevant dcid(s). You have to output only from the given dcids, don't output any other dcids"""
    dcids_list = dspy.InputField(prefix="DCID and Description List: ",desc="DCIDs and its corresponding description")
    relevant_dcids = dspy.OutputField(prefix="Relevant DCIDs: ",desc="relevant dcids only separated by semicolon (;)")

emb_fn = embedding_functions.OpenAIEmbeddingFunction(
    api_key=os.environ["OPENAI_API_KEY"], model_name="text-embedding-3-small"
)

class DataCommonsDSPy(dspy.Module):
    def __init__(self,dcid_collection:chromadb.Collection):
        super().__init__()
        self.datacommons_collection = dcid_collection
        self.place_keyword_llm = dspy.ChainOfThought(PlaceKeywordSignature)
        self.relevant_dcid_llm = dspy.ChainOfThought(SelectDCIDSignature)
        elems = dcid_collection.get()
        langchain_docs = []
        for doc,metadata in zip(elems['documents'],elems['metadatas']):
            
            langchain_docs.append(Document(page_content=doc,metadata=metadata))
        self.bm25_retriever = BM25Retriever.from_documents(
                langchain_docs, k=20, preprocess_func=(lambda x: x.lower())
            )
    def __call__(self,question:str, **kwargs):
        return self.forward(question, **kwargs)
    
    def _where_clause_dcids_helper_func(self,dcids_list:List[str]):
        assert len(dcids_list)>1, "Check the BM25 retriever, the number of returned documents should be more than 1 from the sparse search"
        dcid_where_clause = {"$or": [{"dcid": {"$eq": t}} for t in dcids_list]}
        return dcid_where_clause

    def forward(self,question:Annotated[str,"Question that will be answered by the DataCommons Agent"]):
        question_emb = emb_fn([question])[0]
        llm_answer = self.place_keyword_llm(question=question)
        places = llm_answer.places.split(";")
        places = [pl.strip() for pl in places]
        print()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            results = executor.map(place_dcid,places) 
        place_dcids = []
        for res in results:
            place_dcids.append(res)
        print(place_dcids)
        # Hybrid search (BM25 followed by dense retrieval)
        # bm25_docs = self.bm25_retriever.invoke(question.lower())
        # bm25_dcids = [doc.metadata['dcid'] for doc in bm25_docs]
        # dcid_where_clause = self._where_clause_dcids_helper_func(bm25_dcids)
        dense_retrieval_docs = self.datacommons_collection.query(
            query_embeddings=question_emb,
            # where=dcid_where_clause,
            n_results=5
        )
        select_dcid_str:str = ""
        for dcid_docs, dcid_metadata in zip(
            dense_retrieval_docs["documents"][0], dense_retrieval_docs["metadatas"][0]
        ):
            select_dcid_str+=f"{dcid_metadata['dcid']}: {dcid_docs}\n\n"
        
        print(select_dcid_str)
        relevant_dcid_result = self.relevant_dcid_llm(dcids_list=select_dcid_str)
        relevant_dcid_list = relevant_dcid_result.relevant_dcids.split(";")
        relevant_dcid_list = [rdl.strip() for rdl in relevant_dcid_list]
        print(relevant_dcid_list)
        result_df = dc.build_multivariate_dataframe(place_dcids,relevant_dcid_list)
        return result_df

# dc_chroma = DataCommonsDSPy(dcid_collection)
# dc_chroma("What is the number of patients recovered in COVID-19 from United States and Qatar?")