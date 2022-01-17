import requests 
import pandas as pd
import json
from police_api import PoliceAPI
api = PoliceAPI()
print(api.get_latest_date())

# 1.Get forces
# f = r'https://data.police.uk/api/forces'
forces = [{'id': 'avon-and-somerset', 'name': 'Avon and Somerset Constabulary'}]
# {'id': 'bedfordshire', 'name': 'Bedfordshire Police'} ]
# , {'id': 'cambridgeshire', 'name': 'Cambridgeshire Constabulary'}, {'id': 'cheshire', 'name': 'Cheshire Constabulary'}, {'id': 'city-of-london', 'name': 'City of London Police'}, {'id': 'cleveland', 'name': 'Cleveland Police'}, {'id': 'cumbria', 'name': 'Cumbria Constabulary'}, {'id': 'derbyshire', 'name': 'Derbyshire Constabulary'}, {'id': 'devon-and-cornwall', 'name': 'Devon & Cornwall Police'}, {'id': 'dorset', 'name': 'Dorset Police'}, {'id': 'durham', 'name': 'Durham Constabulary'}, {'id': 'dyfed-powys', 'name': 'Dyfed-Powys Police'}, {'id': 'essex', 'name': 'Essex Police'}, {'id': 'gloucestershire', 'name': 'Gloucestershire Constabulary'}, {'id': 'greater-manchester', 'name': 'Greater Manchester Police'}, {'id': 'gwent', 'name': 'Gwent Police'}, {'id': 'hampshire', 'name': 'Hampshire Constabulary'}, {'id': 'hertfordshire', 'name': 'Hertfordshire Constabulary'}, {'id': 'humberside', 'name': 'Humberside Police'}, {'id': 'kent', 'name': 'Kent Police'}, {'id': 'lancashire', 'name': 'Lancashire Constabulary'}, {'id': 'leicestershire', 'name': 'Leicestershire Police'}, {'id': 'lincolnshire', 'name': 'Lincolnshire Police'}, {'id': 'merseyside', 'name': 'Merseyside Police'}, {'id': 'metropolitan', 'name': 'Metropolitan Police Service'}, {'id': 'norfolk', 'name': 'Norfolk Constabulary'}, {'id': 'north-wales', 'name': 'North Wales Police'}, {'id': 'north-yorkshire', 'name': 'North Yorkshire Police'}, {'id': 'northamptonshire', 'name': 'Northamptonshire Police'}, {'id': 'northumbria', 'name': 'Northumbria Police'}, {'id': 'nottinghamshire', 'name': 'Nottinghamshire Police'}, {'id': 'northern-ireland', 'name': 'Police Service of Northern Ireland'}, {'id': 'south-wales', 'name': 'South Wales Police'}, {'id': 'south-yorkshire', 'name': 'South Yorkshire Police'}, {'id': 'staffordshire', 'name': 'Staffordshire Police'}, {'id': 'suffolk', 'name': 'Suffolk Constabulary'}, {'id': 'surrey', 'name': 'Surrey Police'}, {'id': 'sussex', 'name': 'Sussex Police'}, {'id': 'thames-valley', 'name': 'Thames Valley Police'}, {'id': 'warwickshire', 'name': 'Warwickshire Police'}, {'id': 'west-mercia', 'name': 'West Mercia Police'}, {'id': 'west-midlands', 'name': 'West Midlands Police'}, {'id': 'west-yorkshire', 'name': 'West Yorkshire Police'}, {'id': 'wiltshire', 'name': 'Wiltshire Police'}]
# 2.Get neighbourhouds
all_boundaries=[]
num=0
for i in forces:
    id_force=i["id"]
    neighbourhouds = f'https://data.police.uk/api/{id_force}/neighbourhoods' 
    data1 = requests.get(neighbourhouds)
    data1 = data1.content
    data1 = json.loads(data1)
    
    for j in data1[0:2]:
       
        id_neigh = j["id"]
        
        # 3. get boundary 
        # https://data.police.uk/api/leicestershire/NC04/boundary
        boundaries = f'https://data.police.uk/api/{id_force}/{id_neigh}/boundary'
        data2 = requests.get(boundaries)
        data2 = data2.content
        data2 = json.loads(data2)
        poly=':'.join([f"{p['latitude']},{p['longitude']}" for p in data2])
        print(poly)
        # k = len(api.get_crimes_area(boundaries,date='2013-06'))
        street_level =f'https://data.police.uk/api/crimes-street/all-crime?poly={poly}'
        print(street_level)
        data3 = requests.get(street_level)
        data3 = data3.content
        data3 = json.loads(data3)
        print(len(data3))
        

# %%
# Testing: Get the boundaries for each neghbourhood
import requests
import json
import pandas as pd
boundaries = 'https://data.police.uk/api/leicestershire/NC04/boundary'
data2 = requests.get(boundaries)
data2 = data2.content
data2 = json.loads(data2)
poly=':'.join([f"{p['latitude']},{p['longitude']}" for p in data2])
# https://data.police.uk/api/crimes-street/all-crime?poly=53.7637821,-2.7226353:53.763021,-2.7234077:53.7586067,-2.720232:53.7518067,-2.7234077:53.7493706,-2.7224636:53.7495229,-2.7208328:53.7501319,-2.7171421:53.7485078,-2.714653:53.7475942,-2.7108765:53.7476957,-2.7089024:53.7507917,-2.7028084:53.7516544,-2.6961994:53.7486093,-2.6900196:53.7488631,-2.6852131:53.7531769,-2.6785183:53.7566277,-2.6806641:53.7606363,-2.6795483:53.7612959,-2.6804066:53.7620063,-2.6769733:53.7630717,-2.6775742:53.7693626,-2.6840973:53.7693626,-2.6913071:53.7690583,-2.7052116:53.7671305,-2.7162838:53.7637821,-2.7226353

url = 'https://data.police.uk/api/crimes-street/all-crime'
test_poly_ex ='53.7637821,-2.7226353:53.763021,-2.7234077:53.7586067,-2.720232:53.7518067,-2.7234077:53.7493706,-2.7224636:53.7495229,-2.7208328:53.7501319,-2.7171421:53.7485078,-2.714653:53.7475942,-2.7108765:53.7476957,-2.7089024:53.7507917,-2.7028084:53.7516544,-2.6961994:53.7486093,-2.6900196:53.7488631,-2.6852131:53.7531769,-2.6785183:53.7566277,-2.6806641:53.7606363,-2.6795483:53.7612959,-2.6804066:53.7620063,-2.6769733:53.7630717,-2.6775742:53.7693626,-2.6840973:53.7693626,-2.6913071:53.7690583,-2.7052116:53.7671305,-2.7162838:53.7637821,-2.7226353'
test_poly = ':'.join([f"{p['latitude']},{p['longitude']}" for p in data2])
            


data = {'poly' : test_poly_ex}
street_level =f'https://data.police.uk/api/crimes-street/all-crime?poly={test_poly}'

r = requests.post(url=street_level)
r=r.content
r=json.loads(r)
df = pd.json_normalize(r)
df.to_csv(f'{BASE_DIR}/test/all_urls_lululemon{output_timestamp}.csv')
print(df.shape)
print(df.head())

#  4. Using the boundaries get the crimes at street level
# f5=r'https://data.police.uk/api/crimes-street/all-crime?poly=52.268,0.543:52.794,0.238:52.130,0.478&date=2017-01'
#  getting data for one location which we don't need as we won't data for the whole neghbourhood
# f2 = r"https://data.police.uk/api/crimes-street/all-crime?lat=53.414226&lng=-2.923026&date=2019-08"
# We could get the data as zip but it wont be updated
# f3=r'https://data.police.uk/data/archive/[2017]-[01].zip'

# 5.  save into the landing bucket
from google.cloud import storage
import os
import pandas as pd

# Only need this if you're running this code locally.

client = storage.Client()

bucket = client.get_bucket('landing-bucket)
    
bucket.blob('upload_test/test.csv').upload_from_string(df.to_csv(), 'text/csv')