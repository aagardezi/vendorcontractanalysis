import functions_framework
from google import genai
from google.genai import types
import base64
import json
from google.cloud import bigquery
from datetime import datetime, timezone
from dateutil import parser

def generate(bucketname, filename):
  client = genai.Client(
      vertexai=True,
      project="genaillentsearch",
      location="global",
  )

  msg1_document1 = types.Part.from_uri(
      file_uri=f"""gs://{bucketname}/{filename}""",
      mime_type="application/pdf",
  )
  si_text1 = """You are an agent in the financial team for Pershing Square. You are analysing contracts to and extracting key fields:
Customer Name: 
VendorName: 
ContractTitle: 
EffectiveDate:
TermPeriod:
TerminationDate:
InitialTerm:
RenewalTerm:
TerminationNotificationPeriod:
PaymentTerms:
BillingFrequency:
TotalFees:
UnitRate:
Quantity:
Discount:
QuoteNumber:
AccountNumber:
AgreementNumber:
Currency:
BillingAddress:
Services:
VAT_TaxInformation:
SKUName:

Analyse the entire contract before producing results"""

  model = "gemini-2.5-flash-preview-05-20"
  contents = [
    types.Content(
      role="user",
      parts=[
        msg1_document1,
        types.Part.from_text(text="""Analyse the contract and extract key terms""")
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
    response_schema = {"type":"OBJECT","properties":{"CustomerName":{"type":"STRING"},"VendorName":{"type":"STRING"},"ContractTitle":{"type":"STRING"},"EffectiveDate":{"type":"STRING","format":"date"},"TermPeriod_Months":{"type":"NUMBER"},"TerminationDate":{"type":"STRING","format":"date"},"InitialTerm_MONTHS":{"type":"NUMBER"},"RenewalTerm_MONTHS":{"type":"NUMBER"},"TerminationNotificationPeriod":{"type":"NUMBER"},"PaymentTerms":{"type":"STRING"},"BillingFrequency":{"type":"STRING"},"TotalFees":{"type":"NUMBER"},"UnitRate":{"type":"NUMBER"},"Quantity":{"type":"NUMBER"},"Discount":{"type":"STRING"},"QuoteNumber":{"type":"STRING"},"AccountNumber":{"type":"STRING"},"AgreementNumber":{"type":"STRING"},"Currency":{"type":"STRING"},"BillingAddress":{"type":"STRING"},"Services":{"type":"STRING"},"VAT_TaxInformation":{"type":"STRING"},"SKUName":{"type":"STRING"}},"required":["CustomerName","VendorName","ContractTitle","EffectiveDate","TermPeriod_Months","TerminationDate","InitialTerm_MONTHS","RenewalTerm_MONTHS","TerminationNotificationPeriod","PaymentTerms","BillingFrequency","TotalFees","UnitRate","Quantity","Discount","QuoteNumber","AccountNumber","AgreementNumber","Currency","BillingAddress","Services","VAT_TaxInformation","SKUName"]},
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
    project_id = "############"
    DATASET_ID = "############"
    TABLE_ID = "vendorcontract"

    
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
    data.append({"VendorName": value["VendorName"], "ProcessingDate": datetime.now().isoformat(), "filename": name, "ContractDate": value["EffectiveDate"], "ContractKeyFields": output})
    errors = client.insert_rows_json(f"{project_id}.{DATASET_ID}.{TABLE_ID}", data)
    print(errors)
    print(output)
    return "Completed"

