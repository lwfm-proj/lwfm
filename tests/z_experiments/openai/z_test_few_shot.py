from openai import OpenAI
import sys
client = OpenAI()

completion = client.chat.completions.create(
  model=sys.argv[1],
  messages=[
    {"role": "system", "content": 
     "You are a workflow coding assistant, skilled in using the lwfm python library."},
    {"role": "user", "content": 
     "Write an example python workflow using lwfm which runs a local 'hello world' "\
      "example after another ."}
  ]
)

print(completion.choices[0].message.strip())
