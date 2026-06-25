from fastapi import FastAPI

from scoring.scoring_worker import calc_score
from dtos.dto import ScoreRequest

app = FastAPI()


@app.post("/api/v1/score")
async def start_trip_scoring(request: ScoreRequest):
    print("Received request: ", request)
    calc_score(request)
    return {"message": "Scoring started"}


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}