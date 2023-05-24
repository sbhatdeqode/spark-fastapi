from fastapi import FastAPI
from apis import rdd_apis

app = FastAPI()
app.include_router(rdd_apis.router)

