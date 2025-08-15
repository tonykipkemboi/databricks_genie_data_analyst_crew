#!/usr/bin/env python
import warnings
from data_analyst.crew import DataAnalyst
from datetime import datetime
warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")


def run():
    """
    Run the crew.
    """
    query = input("Enter your query: ")

    inputs = {
        'query': f'{query}',
        'fetch_results_flag': True,
        'date': datetime.now().strftime("%Y-%m-%d")
    }
    
    try:
        DataAnalyst().crew().kickoff(inputs=inputs)
    except Exception as e:
        raise Exception(f"An error occurred while running the crew: {e}")

if __name__ == "__main__":
    run()
