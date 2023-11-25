from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import concurrent.futures

app = FastAPI()

class DataRequest(BaseModel):
    rows: int
    fields: int

class DataProcessor:
    def __init__(self, data):
        self.data = data

    def clean_data(self):
        cleaned_data = self.data.dropna()
        return cleaned_data

def process_data_in_chunks(data, rows, fields):
    chunks = [data[i:i+rows] for i in range(0, len(data), rows)]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        processed_chunks = list(executor.map(DataProcessor, chunks))

    combined_data = pd.concat([chunk.clean_data() for chunk in processed_chunks])

    return combined_data.head(fields).to_json(orient='records')

@app.post("/ingest-data")
async def ingest_data(request_data: DataRequest):
    data = pd.read_csv("/content/dataset.csv")

    processed_data = process_data_in_chunks(data, request_data.rows, request_data.fields)

    return {"processed_data": processed_data}