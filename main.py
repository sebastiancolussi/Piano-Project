import pandas as pd
import os
import json
import requests


# -----------Global Variables---------------------------------------------------
MAIN_PATH = os.getcwd()
PATH_TO_CONFIG = '\\'.join((MAIN_PATH, 'config.json'))
PATH_TO_FILE_A = '\\'.join((MAIN_PATH, 'File_A.csv'))
PATH_TO_FILE_B = '\\'.join((MAIN_PATH, 'File_B.csv'))

# Get app config from json
with open(PATH_TO_CONFIG) as cfg:
    CONFIG = json.load(cfg)


# -----------Read users data files----------------------------------------------
file_a = pd.read_csv(PATH_TO_FILE_A)
file_b = pd.read_csv(PATH_TO_FILE_B)
merged_file = pd.merge(file_a, file_b, on=['user_id'])


# -----------Make HTTP requests to API------------------------------------------
ENDPOINT = '/publisher/user/list'

payload = {
    "api_token": CONFIG['TOKEN'],
    "aid": CONFIG['AID'],
    "limit": 1000,
    "offset": 0
}

response = requests.post(url=CONFIG['API_PATH'] + ENDPOINT, params=payload)
users = pd.DataFrame(json.loads(response.content)['users'])


# -----------Replace user_id in merged file (for existing users)----------------
entries_replaced = 0
for row in users.iterrows():
    print(row)
    mask = row[1]['email'] == merged_file['email']

    # Replace uid if necessary
    if mask.any():
        prev_uid = merged_file['user_id'][mask].iloc[0]
        new_uid = row[1]['uid']
        merged_file.replace(prev_uid, new_uid, inplace=True)

        entries_replaced += 1


# -----------Write results to file----------------------------------------------
PATH_TO_MERGED_FILE = MAIN_PATH + '\\merged_file.csv'
merged_file.to_csv(PATH_TO_MERGED_FILE, index=False)

print('\nRESULTS: ' + str(entries_replaced) + ' entries were replaced')
