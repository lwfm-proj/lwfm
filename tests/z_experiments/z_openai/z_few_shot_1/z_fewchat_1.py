# nice... turns out this is deprecated code...

from langchain import HuggingFaceHub

customer_email = """
Hi, I am writing to you to ask for a refund. I bought your product last week and it is not working as expected. I would like to return it and get my money back. Please let me know how I can do this. Thank you.
"""

summarizer = HuggingFaceHub(
    repo_id="facebook/bart-large-cnn",
    model_kwargs={"temperature":0, "max_length":180}
)
def summarize(llm, text) -> str:
    return llm(f"Summarize this: {text}!")

summarize(summarizer, customer_email)