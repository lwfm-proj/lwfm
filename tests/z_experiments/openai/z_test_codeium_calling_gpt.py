# asked for an example of calling a gpt on openai 
# on execution we're told this is an old SDK signature
# notice also there is no place to enter the token  

from openai import OpenAI

# Instantiate the OpenAI client
client = OpenAI()

# Define the GPT model you want to use
model_name = "gpt-3.5-turbo"

# Define the prompt for the GPT model
prompt = "Translate the following English text to French: '{}'"
text_to_translate = "Hello, World!"
full_prompt = prompt.format(text_to_translate)

# Call the GPT model with the prompt
response = client.completions.create(
  model=model_name,
  prompt=full_prompt,
  max_tokens=60
)

# Print the translated text
print("Hello folks, I'm translating from English to French.")
print(response.choices[0].text.strip())
