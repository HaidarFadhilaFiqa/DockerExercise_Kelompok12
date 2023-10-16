import requests 
from bs4 import BeautifulSoup
import pandas as pd 
import psycopg2
from sqlalchemy import create_engine

host = 'ep-silent-lab-755834.ap-southeast-1.aws.neon.tech'
port = 5432
database = 'dibimbing'
username = 'students'
password = 'Jeruk@2023'

# Untuk simpan environment database
host_1 = '127.0.0.1'
port_1 = 5432
database_1 = 'test'
username_1 = 'test'
password_1 = 'test'

connection = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)

url_country = 'https://www.petsecure.com.au/pet-care/a-guide-to-worldwide-pet-ownership/'
url = 'https://api.opencagedata.com/geocode/v1/json'

api_key = 'a34768ccd55d49cfa29fb5753e2d1486'

# ekstrak data country
response = requests.get(url_country)
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find('table',{'class':'cats'})
headers = []
for th in table.find_all('th'):
  headers.append(th.text.strip())
rows = []
for tr in table.find_all('tr'): #loop komponen tr
  row_data = []
  for td in tr.find_all('td'):
    row_data.append(td.text.strip())
  if len(row_data) > 0:
    rows.append(row_data)
population_df = pd.DataFrame(rows,columns=headers)
population_df = population_df.rename(columns={population_df.columns[0]:'country',population_df.columns[1]:'populations'})

print('Ekstrak country selesai')

# ekstrak data country code
countries = population_df['country'].to_list()
countries_list = []
for country in countries:
  params = {'q': country, 'key': api_key}
  response = requests.get(url,params=params)
  json_data = response.json()
  components = json_data['results'][0]['components']
  geometry = json_data['results'][0]['geometry']
  country_components = {
      'country': country,
      'country_code': components.get('country_code',''),
      'latitude': geometry.get('lat'),
      'longitude': geometry.get('lng')
  }
  countries_list.append(country_components)
component_df = pd.DataFrame(countries_list)

print('Ekstrak country code selesai')

# ambil data pet_stores
query = 'SELECT * FROM public.pet_stores'
pet_stores_df = pd.read_sql(query,connection)

print('Ambil data pet_stores selesai')

# TRANSFORMASI
df = pd.merge(pet_stores_df,pd.merge(population_df,component_df,on='country'),on='country')
print('Merge data selesai')

df['country_code'] = df['country_code'].str.upper()
print('Ubah jadi upper selesai')

df['populations'] = df['populations'].str.replace(',','').astype(int)
print('Ubah tipe data selesai')

df['total_cat_per_store'] = df['populations']/df['total_pet_store']
print('Data enrichment selesai')

top_5_df = df.sort_values(by='total_cat_per_store',ascending=False).head(5)
print('Sort data selesai')

# LOAD DATA
engine = create_engine(f'postgresql://{username_1}:{password_1}@{host_1}:{port_1}/{database_1}')

top_5_df.to_sql(name='top_5_cat_per_store', con=engine, if_exists='replace', index=False)

engine.dispose()

print("Load data selesai")