# example of calling a gpt taken from the openai documentation
# notice no place to enter the token - the environ

from openai import OpenAI
client = OpenAI()

model_list = client.models.list()

#for i in model_list:
#    print(i)

completion = client.chat.completions.create(
    model="gpt-3.5-turbo",
  messages=[
    {"role": "system", "content": "You are a poetic assistant, skilled in explaining complex programming concepts with creative flair."},
    {"role": "user", "content": "Compose a poem that explains the concept of recursion in programming."}
])

print(completion.choices[0].message)