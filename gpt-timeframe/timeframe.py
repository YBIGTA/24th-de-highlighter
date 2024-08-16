import openai
import os
import pandas as pd

# load_dotenv()
api_key = ""

# Initialize the OpenAI API
openai.api_key = api_key

client = openai()

# Make the API call
response = client.chat.completions.create(
  model="gpt-4o-mini",
  messages=[
    {"role": "system", "content": "Who is Heungindaeyo?"}
  ]
)

# Print the response
print(response.choices[0].message.content)

from openai import OpenAI
client = OpenAI()

completion = client.chat.completions.create(
  model="gpt-4o-mini",
  messages=[
    {"role": "system", "content": "Who is Heungindaeyo?"},
  ]
)

print(completion.choices[0].message)
