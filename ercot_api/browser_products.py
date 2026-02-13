from client import ErcotPublicDataClient
import zipfile
import io
import os
import glob
from tqdm import tqdm

USERNAME = "williamzhongkaiwu@gmail.com"
PASSWORD = "Wyslmwinlyab5225"
# client = ErcotPublicDataClient(api_key="a2255a08961b41f187af0e7a248fb2d7", api_key_in_query=False)
client = ErcotPublicDataClient(api_key="2149cf06d17d456bbe98e98fffa60ac0", api_key_in_query=False)
client.authenticate(USERNAME, PASSWORD)
products = client.list_products()