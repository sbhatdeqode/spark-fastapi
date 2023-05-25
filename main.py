from fastapi import FastAPI
from apis import rdd_apis, sql_apis

app = FastAPI()
app.include_router(rdd_apis.router)
app.include_router(sql_apis.router)

