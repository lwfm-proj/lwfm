from openai import OpenAI
client = OpenAI()
# Upload a file with an "assistants" purpose
file1 = client.files.create(
  file=open("src/lwfm/base/Site.py", "rb"),
  purpose='assistants'
)
file2 = client.files.create(
  file=open("src/lwfm/examples/ex0_hello_world.py", "rb"),
  purpose='assistants'
)
file3 = client.files.create(
  file=open("src/lwfm/examples/ex1_job_triggers.py", "rb"),
  purpose='assistants'
)

# Create an assistant using the file ID
assistant = client.beta.assistants.create(
  instructions="You are a workflow coding assistant. When asked to write a workflow, "\
    "write code using the Site class and use the lwfm library based on the examples provided.",
  model="gpt-4-1106-preview",
  tools=[{"type": "code_interpreter"}],
  file_ids=[file1.id, file2.id, file3.id]
)

# Create a thread where the conversation will happen
thread = client.beta.threads.create()

# Create the user message and add it to the thread
message1 = client.beta.threads.messages.create(
    thread_id=thread.id,
    role="user",
    content="I need to write a workflow to echo 'hello world'. "\
        "Can you show me the Python code using the lwfm.Site class?")

message2 = client.beta.threads.messages.create(
    thread_id=thread.id,
    role="user",
    content="I need to write a local workflow to echo "\
      "'hello world' after another lwfm job which echoes 'hello world' runs. "\
      "Can you show me the Python code using the lwfm.Site class?")      

# Create the Run, passing in the thread and the assistant
run = client.beta.threads.runs.create(
  thread_id=thread.id,
  assistant_id=assistant.id
)

# Periodically retrieve the Run to check status and see if it has completed
# Should print "in_progress" several times before completing
while run.status != "completed":
    keep_retrieving_run = client.beta.threads.runs.retrieve(
        thread_id=thread.id,
        run_id=run.id
    )
    print(f"Run status: {keep_retrieving_run.status}")

    if keep_retrieving_run.status == "completed":
        print("\n")
        break

# Retrieve messages added by the Assistant to the thread
all_messages = client.beta.threads.messages.list(
    thread_id=thread.id
)

# Print the messages from the user and the assistant
print("###################################################### \n")
print(f"USER: {message1.content[0].text.value}")
print(f"USER: {message2.content[0].text.value}")
print(f"ASSISTANT: {all_messages.data[0].content[0].text.value}")

