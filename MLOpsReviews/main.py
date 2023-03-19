from fastapi import FastAPI
from routes.user import user
import pandas as pd
import numpy as np

app = FastAPI()

app.include_router(user)
