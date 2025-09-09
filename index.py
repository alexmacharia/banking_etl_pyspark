import pandas as pd
import numpy as np
from sqlalchemy import create_engine

df = pd.createDataFrame({'name': ['alex','maina'], 'age': [40, 41]})

df_2 = pd.read_sql('select * from cases')

