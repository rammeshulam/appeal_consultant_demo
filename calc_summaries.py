# pip install virtualenv
# virtualenv ram_google
# source ram_google/bin/activate
# ram_google/bin/pip install google-cloud-bigquery
# pip install pandas
# pip install pandas_gbq
# pip install google-generativeai

#install google-cloud sdk
#gcloud auth application-default login
# to activate: source ram_google/bin/activate

# cd Downloads/appeal_demo/
# source ram_google/bin/activate
# python3 ./calc_summaries.py 5

from sys import argv
from google.cloud import bigquery
import pandas as pd
import re
import pprint
import json
import pandas_gbq as gbq
import json
import time

import os
import google.generativeai as genai
from google.ai.generativelanguage_v1beta.types import content
from google.generativeai.types import HarmCategory, HarmBlockThreshold

#from google.colab import auth
#auth.authenticate_user()

GEMINI_API_KEY = '<GEMINI_API_KEY>'
projectid = '<PROJECT_ID>'

project_dir = '<PROJECT_DIR>'
DRIVE_DIR = '<DRIVE_DIR>'

# read main crime categories from file and create an enum accordingly:
crimes_categories_file = project_dir + 'crime_categories.txt'
with open(crimes_categories_file) as f:
  crime_categories = [line.rstrip('\n') for line in f]


prompt_path = project_dir + 'prompt.txt'
with open(prompt_path) as f:
  prompt = f.read()

#Clean dataset for data processing
verdicts_crime_clean_file_name = 'verdicts_crime_clean.csv'
verdicts_crime_clean_path = project_dir + verdicts_crime_clean_file_name
verdicts_crime_clean_cache_path = DRIVE_DIR + verdicts_crime_clean_file_name


# read main crime categories from file and create an enum accordingly:
crimes_categories_file = project_dir + 'crime_categories.txt'
with open(crimes_categories_file) as f:
  crime_categories = [line.rstrip('\n') for line in f]


prompt_path = project_dir + 'prompt.txt'
with open(prompt_path) as f:
  prompt = f.read()

"""
Install the Google AI Python SDK

$ pip install google-generativeai
$ pip install google.ai.generativelanguage
"""

genai.configure(api_key=GEMINI_API_KEY)

from google.ai.generativelanguage_v1beta.types import content

crime_categories_schema = content.Schema(type = content.Type.STRING, 
                                         format = 'enum', 
                                         enum = crime_categories)
# Create the model
generation_config = {
  "temperature": 0,
  "max_output_tokens": 8192,
  "response_schema": content.Schema(
    type = content.Type.OBJECT,
    properties = {
      "response": content.Schema(
        type = content.Type.OBJECT,
        properties = {
          "summary": content.Schema(
            type = content.Type.STRING,
          ),
          "crime_categories": content.Schema(
              type = content.Type.ARRAY, 
              items = crime_categories_schema,
              description = "סיווג ההאשמות בתיק לקטגוריה (בין 1-3 קטגוריות, מסודרות לפי חשיבות)"
          ),
#          "crime_category2": crime_categories_schema,
#          "crime_category3": crime_categories_schema,
          "reason_for_appeal": content.Schema(
            type = content.Type.STRING,
            description = "Why is the case being raised? for appeal"
          ),
          "is_punishment_does_not_match_crime": content.Schema(
            type = content.Type.BOOLEAN,
            description = "האם הערעור טוען שהעונש אינו תואם לחומרת העבירה?"
          ),
          "is_extenuating_circumstances": content.Schema(
            type = content.Type.BOOLEAN,
            description = "האם הערעור מבקש להתחשב בנסיבות מיוחדות בהן בוצע הפשע?"
          ),
          "is_personal_circumstances": content.Schema(
            type = content.Type.BOOLEAN,
            description = "האם הערעור מבקש להתחשב בנסיבות המיוחדות של הנאשם?"
          ),
          "is_new_evidence": content.Schema(
            type = content.Type.BOOLEAN,
            description = "האם הערעור נובע מראיות או עדויות חדשות שעלו מאז גזר הדין?"
          ),
          "sections_of_Penal_Code" : content.Schema(
            type = content.Type.STRING,
            description = "סעיפים מחוק העונשין המופיעים בערעור"
          ),
          "punishment": content.Schema(
            type = content.Type.STRING,
            description = "גזר הדין בערעור"
          ),
        },
      ),
    },
  ),
  "response_mime_type": "application/json",
}

def get_model():
  model = genai.GenerativeModel(
    #model_name="gemini-1.5-flash",
    model_name="gemini-1.5-pro-latest",
    #model_name="gemini-1.5-pro-exp-0801",
    #model_name="gemini-1.5-pro",
    generation_config=generation_config,
    # See https://ai.google.dev/gemini-api/docs/safety-settings
    safety_settings={
          HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
          HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
          HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
          HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE
          }
    )
  return model

model = get_model()

def update_bq_with_suammry(client, CaseId, summary):
  query = (
      """UPDATE appeal_consultant_demo.verdicts 
      SET summary = @summary
      WHERE CaseId=@CaseId""")
  job_config = bigquery.QueryJobConfig(
      query_parameters=[
          bigquery.ScalarQueryParameter("summary", "STRING", summary),
          bigquery.ScalarQueryParameter("CaseId", "INT64", CaseId),
      ]
  )
  query_job = client.query(query, job_config=job_config)  # API request
  query_job.result()
  if(query_job.state=='DONE'):   # Waits for statement to finish
    return True
  return False

def fetch_ids_to_process(client):
  return client.query("select CaseId from appeal_consultant_demo.verdicts where summary='n/a'").to_dataframe().CaseId.tolist()

def update_bq_with_suammries(client, df_to_process, shard):
  model = get_model()
  for index, row in df_to_process.iterrows():
    if(row.CaseId%10!=shard):
      print(f'skipping. shard {shard}, CaseId {row.CaseId}')
      continue
    chat_session = model.start_chat(history=[])
    print(f"shard {shard}: Processing CaseId: {str(row.CaseId)}")
    response = chat_session.send_message(prompt + "\n\n" + row.text)
    print("resonse: " + response.text)
    try:
      update_bq_with_suammry(client, row.CaseId, response.text)
    except:
      print(">>>> Failed to update DB. Waiting <<<")
      time.sleep(5)
    print(row.CaseId)

def main():
  shard = int(argv[1])
  print(f"shard: {shard}")
  client = bigquery.Client(project = projectid)
  df = pd.read_csv(verdicts_crime_clean_path)
  ids = fetch_ids_to_process(client)
  df_to_process = df.loc[df.CaseId.isin(ids)] 
  update_bq_with_suammries(client, df_to_process, shard)
    

if __name__ == "__main__":
    main()