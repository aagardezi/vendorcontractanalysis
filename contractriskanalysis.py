import functions_framework
from google import genai
from google.genai import types
import base64
import json
from google.cloud import bigquery
from datetime import datetime, timezone
from dateutil import parser

PROJECT_ID = "genaillentsearch"
DATASET_ID = "#############"
TABLE_ID = "vendorcontractrisk"
LOCATION = "gloabl"

def generate(bucketname, filename):
  client = genai.Client(
      vertexai=True,
      project=PROJECT_ID,
      location=LOCATION,
  )

  msg1_document1 = types.Part.from_uri(
      file_uri=f"""gs://{bucketname}/{filename}""",
      mime_type="application/pdf",
  )
  si_text1 = """You are an agent in the financial team for financial organisations. You are analysing contracts identify risks"""

  model = "gemini-2.5-pro-preview-06-05"
  contents = [
    types.Content(
      role="user",
      parts=[
        msg1_document1,
        types.Part.from_text(text="""Can you identify key risks in this contract, do a detailed analysis.""")
      ]
    ),
  ]

  generate_content_config = types.GenerateContentConfig(
    temperature = 1,
    top_p = 1,
    seed = 0,
    max_output_tokens = 65535,
    safety_settings = [types.SafetySetting(
      category="HARM_CATEGORY_HATE_SPEECH",
      threshold="OFF"
    ),types.SafetySetting(
      category="HARM_CATEGORY_DANGEROUS_CONTENT",
      threshold="OFF"
    ),types.SafetySetting(
      category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
      threshold="OFF"
    ),types.SafetySetting(
      category="HARM_CATEGORY_HARASSMENT",
      threshold="OFF"
    )],
    response_mime_type = "application/json",
    response_schema = {"type":"OBJECT","properties":{"vendor_name":{"type":"STRING"},"contract_title":{"type":"STRING"},"risks":{"type":"ARRAY","items":{"type":"OBJECT","properties":{"risk_title":{"type":"STRING"},"location_in_file":{"type":"OBJECT","properties":{"description":{"type":"STRING"},"page_number":{"type":"NUMBER"},"line_number":{"type":"NUMBER"}}},"clause_analysis":{"type":"STRING"},"implication_for_client":{"type":"STRING"},"recommendation":{"type":"STRING"}},"required":["risk_title","location_in_file","clause_analysis","implication_for_client","recommendation"]}}},"required":["vendor_name","contract_title","risks"]},
    system_instruction=[types.Part.from_text(text=si_text1)],
  )

  response = client.models.generate_content(
    model = model,
    contents = contents,
    config = generate_content_config,
    )
  return response.text


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    #This is what you need to set
    project_id = PROJECT_ID
    # DATASET_ID = DATASET_ID
    # TABLE_ID = "vendorcontract"

    
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    timestamp = cloud_event["time"]

    event_time = parser.parse(timestamp)
    event_age = (datetime.now(timezone.utc) - event_time).total_seconds()
    event_age_ms = event_age * 1000

    # Ignore events that are too old
    max_age_ms = 10000
    if event_age_ms > max_age_ms:
        print("Dropped {} (age {}ms)".format(cloud_event["id"], event_age_ms))
        return "Timeout"

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    output = generate(bucket, name)
    value = json.loads(output)
    client = bigquery.Client()
    data = []
    data.append({"VendorName": value["VendorName"], "ProcessingDate": datetime.now().isoformat(), "Filename": name, "ContractRisks": output})
    errors = client.insert_rows_json(f"{project_id}.{DATASET_ID}.{TABLE_ID}", data)
    print(errors)
    print(output)
    return "Completed"

