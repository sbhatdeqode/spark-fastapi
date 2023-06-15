from fastapi import FastAPI
from apis import rdd_apis, sql_apis
import os,sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
app = FastAPI()
app.include_router(rdd_apis.router)
app.include_router(sql_apis.router)

