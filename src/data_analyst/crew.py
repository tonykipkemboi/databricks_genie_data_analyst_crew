from crewai import Agent, Crew, Process, Task, LLM
from crewai.project import CrewBase, agent, crew, task
from typing import List
import os

from .tools.databricks_tools import DatabricksGenieNLQTool

@CrewBase
class DataAnalyst():
    """DataAnalyst crew"""

    agents_config = 'config/agents.yaml'
    tasks_config = 'config/tasks.yaml'

    @agent
    def databricks_query_agent(self) -> Agent:
        databricks_tool = DatabricksGenieNLQTool()
        return Agent(
            config=self.agents_config['databricks_query_agent'],
            tools=[databricks_tool],
            llm=LLM(model="gpt-4o-mini"),
            verbose=True
        )

    @task
    def natural_language_db_query_task(self) -> Task:
        return Task(
            config=self.tasks_config['natural_language_db_query_task'],
            markdown=True,
            output_file="output/databricks_query_output.md"
        )

    @crew
    def crew(self) -> Crew:
        return Crew(
            agents=self.agents, 
            tasks=self.tasks,  
            process=Process.sequential,
            verbose=True
        )
