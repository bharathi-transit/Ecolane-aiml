import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys

# Add src to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.pbi_agent.cli import build_agent
from src.pbi_agent.agent import is_junk_query
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

agent = build_agent(project_root)

class Query(BaseModel):
    question: str

@app.post("/query")
async def query(query: Query):
    if is_junk_query(query.question):
        return {"response": "Hello! I'm your Power BI assistant. How can I help you with your KPIs today?"}
    
    result = agent.invoke({"question": query.question})
    
    # Check if the result contains a fallback message and return it
    if result.get("fallback"):
        return {"response": result.get("response")}
        
    return result
