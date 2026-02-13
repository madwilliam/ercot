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
# client.update_archive(product_id = 'np4-33-cd')
product_id = 'np4-33-cd'
report = client.get_product_history_bundles(product_id)
ids = [i['docId'] for i in report['bundles']]
max_retry = 3
for report_id in tqdm(ids):
    done = False
    retry = 0
    while not done:
        try:
            zip = client.download_bundle(product_id, [report_id])
            bytes_io = io.BytesIO(zip)
            save_dir = f'/home/dell/code/ercot/ercot/data/{product_id}/{product_id}_{str(report_id)}'
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)
                with zipfile.ZipFile(bytes_io, 'r') as zip_ref:
                    zip_ref.extractall(save_dir)
                files = os.listdir(save_dir)
                for filei in files:
                    file_path = os.path.join(save_dir,filei)
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(save_dir)
                path_pattern = f"{save_dir}/*.zip"
                for file_path in glob.glob(path_pattern):
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
            else:
                print(f'{report_id} already exist')
            done = True
        except:
            if retry<max_retry:
                print('retrying')
            else:
                break
            retry+=1
