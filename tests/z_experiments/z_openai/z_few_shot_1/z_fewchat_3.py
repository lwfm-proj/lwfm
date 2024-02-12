from langchain.agents import initialize_agent
from langchain.agents import Tool
import os
import sys
import time
import logging
import random
import argparse
from pathlib import Path

from langchain.chat_models import ChatOpenAI
from langchain.chains.conversation.memory import ConversationBufferWindowMemory

from langchain_tools.lwfm_langchain_tool import LwfmTool
from langchain_tools.util_tool import UtilTool

#os.environ['REQUESTS_CA_BUNDLE'] = 'C:\\Users\\gr80m\\anaconda3\\envs\\langchain\\Lib\\site-packages\\certifi\\cacert.pem'
class LwfmAssistant():

	def __init__(self, vars):

		os.environ['OPENAI_API_KEY'] = vars['openai']
		os.environ["GOOGLE_CSE_ID"] = vars['google_cse']
		os.environ["GOOGLE_API_KEY"] = vars['google']

		turbo_llm = ChatOpenAI(
			temperature=0,
			model_name='gpt-3.5-turbo'
		)

		memory = ConversationBufferWindowMemory(
			memory_key="chat_history",
			k=10,
			return_messages=True
		)

		self.util = UtilTool()
		self.lwfm = LwfmTool()

		tools = [self.lwfm.login_tool, self.lwfm.upload_tool, self.lwfm.download_tool, self.lwfm.find_tool, self.lwfm.submit_job_tool, self.util.command_tool, self.util.google_tool, self.util.random_number_tool]

		self.conversational_agent = initialize_agent(
			agent='chat-conversational-react-description',
			tools=tools,
			llm=turbo_llm,
			verbose=True,
			max_iterations=20,
			early_stopping_method='generate',
			memory=memory
		)

	def runChatbot(self, template=None, template_input=None, project_context=None):
		print
		if project_context:
			context = self.read_file_into_string(project_context)
			self.conversational_agent("Do not call any of the tools just yet.  Just remember the following for future reference:" + self.read_file_into_string(project_context))
		if template:
			# if template_input:
			# 	self.conversational_agent("I have this python dictionary which contains a list of generic parameter fields and values that could be referenced by the user for anything, remember it for future reference " + str(template_input))
			user_lines = self.read_file_to_list(template)
			for line in user_lines:
				print("User: " + line)
				self.conversational_agent(line)
		else:
			user_responses = []
			print("Chatbot: Hello, How can I assist you today?")
			while True:
				user_input = input("User: ")
				if user_input.lower() == "exit":
					break
				elif user_input.lower() == "save":
					file_path = input("Where would you like to save the chat template file: ")
					self.write_list_to_file(file_path, user_responses)
				else:
					user_responses.append(user_input)
					self.conversational_agent(user_input)

	def read_file_to_list(self, file_path):
	    lines_list = []
	    with open(file_path, 'r') as file:
	        for line in file:
	            lines_list.append(line.strip())
	    return lines_list

	def read_file_into_string(self, file_path):
	    try:
	        with open(file_path, 'r') as file:
	            content = file.read()
	        return content
	    except FileNotFoundError:
	        return "File not found."
	    except Exception as e:
	        return f"An error occurred: {str(e)}"

	def write_list_to_file(self, file_path, lines_list):
	    with open(file_path, 'w') as file:
	        for line in lines_list:
	            file.write(line + '\n')

if __name__ == '__main__':
	print("running main")

	parser = argparse.ArgumentParser(description='''The LWFM assistant.  This is an openai chatbot that can make lwfm calls if the user intructs it to do so.''')

	parser.add_argument('openai', type=str, help='OpenAi token used to connect to OpenAi')
	parser.add_argument('--google', type=str, help='Google token used to connect to Google Ai')
	parser.add_argument('--google_cse', type=str, help='ID of the google search engine you would like to use.')
	parser.add_argument('--template', type=str, help='A template from a previously saved conversation with lwfm assistamnt to be reran (essentially a workflow).')
	parser.add_argument('--template_input', type=dict, help='A dictionary of paramters that can be used within the template.  The template can reverence them with {{paramName}}')
	parser.add_argument('--project_context', type=str, help='A dictionary of paramters that can be used within the template.  The template can reverence them with {{paramName}}')

	parser.set_defaults(google=None, google_cse=None, template=None, template_input=None, project_context=None)

	args = vars(parser.parse_args())

	template = args["template"]
	template_input = args["template_input"]
	project_context = args["project_context"]

	#for i in range(5):
	chatbot = LwfmAssistant(args)
	template_input = {"outputFileName":"outputFile"}
	chatbot.runChatbot(template, template_input, project_context)