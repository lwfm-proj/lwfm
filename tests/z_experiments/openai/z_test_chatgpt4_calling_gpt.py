# asked copilot for an example of calling a gpt on openai 
# on execution we're told this is an old SDK signature...

import openai

# Set your OpenAI API key
openai.api_key = 'my-key'  # this is contraindicated - hardcoded credentials!

# Make a request to the askthecode_gpt model
response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is the capital of France?"}
    ],
    gpt=3,
    temperature=0.7,
    max_tokens=100,
    stop=None
)

# Print the response
print(response)
