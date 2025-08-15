# DataAnalyst Crew

Welcome to the DataAnalyst Crew project, powered by [crewAI](https://crewai.com). This template is designed to help you set up a multi-agent AI system with ease, leveraging the powerful and flexible framework provided by crewAI. Our goal is to enable your agents to collaborate effectively on complex tasks, maximizing their collective intelligence and capabilities.

Watch a short walkthrough of the tool in action:

![Watch the demo](assets/demo-recording.gif)

## Installation

Ensure you have Python >=3.10 <3.13 installed on your system. This project uses [UV](https://docs.astral.sh/uv/) for dependency management and package handling, offering a seamless setup and execution experience.

First, if you haven't already, install uv:

```bash
pip install uv
```

Next, navigate to your project directory and install the dependencies:

(Optional) Lock the dependencies and install them by using the CLI command:
```bash
crewai install
```
### Customizing

**Add your `OPENAI_API_KEY` into the `.env` file**

- Modify `src/data_analyst/config/agents.yaml` to define your agents
- Modify `src/data_analyst/config/tasks.yaml` to define your tasks
- Modify `src/data_analyst/crew.py` to add your own logic, tools and specific args
- Modify `src/data_analyst/main.py` to add custom inputs for your agents and tasks

## Running the Project

To kickstart your crew of AI agents and begin task execution, run this from the root folder of your project:

```bash
$ crewai run
```

This command initializes the data-analyst Crew, assembling the agents and assigning them tasks as defined in your configuration.

This example, unmodified, will run the create a `report.md` file with the output of a research on LLMs in the root folder.

## Databricks Genie setup (.env-based)

Configure the tool entirely via your `.env` file. No manual exports required at runtime.

### 1) Required variables (PAT-only)

Add the following to `.env` (in the project root):

```bash
# Workspace host (no protocol). Example: adb-12345.9.azuredatabricks.net or <workspace>.cloud.databricks.com
DATABRICKS_INSTANCE=

# Genie Space ID (see how to find below)
GENIE_SPACE_ID=

DATABRICKS_TOKEN=

# OAuth variables are no longer supported by this tool. Use a Workspace PAT instead.
```

Notes:
- The tool auto-detects OAuth if `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and `DATABRICKS_REDIRECT_URI` are set; otherwise it uses `DATABRICKS_TOKEN`.
- Tokens are not printed in logs. Keep your `.env` out of version control.

### 2) How to find values

- DATABRICKS_INSTANCE: the hostname in your browser URL when in Databricks (no `https://`).
- GENIE_SPACE_ID: in the Databricks UI, open Genie → click your Space → copy from URL `.../genie/spaces/<space_id>/...`.
  - Or via API using a PAT:
  ```bash
  curl -s https://$DATABRICKS_INSTANCE/api/2.0/genie/spaces \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" | jq
  ```

### 3) Run and verify

1. Ensure `.env` is filled and saved.
2. Start the crew:
   ```bash
   crewai run
   ```
3. No consent prompts are shown; PAT is used directly.

Optional verification (PAT example):
```bash
curl -i -X POST "https://$DATABRICKS_INSTANCE/api/2.0/genie/spaces/$GENIE_SPACE_ID/start-conversation" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" -H "Content-Type: application/json" \
  -d '{"content":"hello"}'
```

### 4) Troubleshooting

- 403 Invalid access token: generate a new Workspace PAT for the same `DATABRICKS_INSTANCE` and update `.env`.
- Wrong workspace/space: double-check `DATABRICKS_INSTANCE` and `GENIE_SPACE_ID` match the Space you see in the UI.
- If you see 401/403 errors, verify that the PAT is for this workspace host and that you have permission on the Genie space.

## Understanding Your Crew

The data-analyst Crew is composed of multiple AI agents, each with unique roles, goals, and tools. These agents collaborate on a series of tasks, defined in `config/tasks.yaml`, leveraging their collective skills to achieve complex objectives. The `config/agents.yaml` file outlines the capabilities and configurations of each agent in your crew.

## Support

For support, questions, or feedback regarding the DataAnalyst Crew or crewAI.
- Visit our [documentation](https://docs.crewai.com)
- Reach out to us through our [GitHub repository](https://github.com/joaomdmoura/crewai)
- [Join our Forum](https://community.crewai.com)
- [Chat with our docs](https://chatg.pt/DWjSBZn)

Let's create wonders together with the power and simplicity of crewAI.
