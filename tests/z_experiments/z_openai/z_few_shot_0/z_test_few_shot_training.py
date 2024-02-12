from openai import OpenAI
import uuid

client = OpenAI()

client.files.create(
  file=open("src/lwfm/base/Site.py", "rb"),
  purpose="fine-tune"
)
client.files.create(
  file=open("src/examples/ex0_hello_world.py", "rb"),
  purpose="fine-tune"
)
client.files.create(
  file=open("src/examples/ex1_job_triggers.py", "rb"),
  purpose="fine-tune"
)

client.fine_tuning.jobs.create(
  training_file=str(uuid.uuid4())[:8],
  model="gpt-3.5-turbo"
)


