import os
import time
import requests
from dotenv import load_dotenv
from typing import Type, Optional
from pydantic import BaseModel, Field
from crewai.tools import BaseTool
 

load_dotenv()

class DatabricksGenieInput(BaseModel):
    """Input schema for the Databricks Genie NLQ Tool."""
    natural_language_query: str = Field(..., description="The natural language question to ask Databricks Genie.")
    conversation_id: Optional[str] = Field(None, description="Optional ID of an existing conversation to continue. If None, a new conversation is started.")
    fetch_query_results: bool = Field(False, description="If True, the tool will attempt to fetch and return the actual data results of the generated SQL query. Defaults to False.")
    polling_interval_seconds: int = Field(5, description="How often (in seconds) to poll for Genie's response status. Defaults to 5 seconds.")
    polling_timeout_seconds: int = Field(600, description="Maximum time (in seconds) to wait for Genie to complete processing. Defaults to 600 seconds (10 minutes).")

class DatabricksGenieNLQTool(BaseTool):
    name: str = "Databricks Genie NLQ"
    description: str = (
        "Queries a Databricks Genie space using natural language. "
        "Uses a Databricks Personal Access Token (PAT) for authentication. "
        "Set DATABRICKS_TOKEN, DATABRICKS_INSTANCE, and GENIE_SPACE_ID environment variables. "
        "It requires DATABRICKS_INSTANCE and GENIE_SPACE_ID environment variables to be set. "
        "It can start a new conversation or continue an existing one. "
        "It retrieves the generated SQL query and Genie's textual response. "
        "Optionally, it can also fetch the data results of the executed query. "
        "Returns the conversation ID, Genie's response, SQL query, and optionally data results."
    )
    args_schema: Type[BaseModel] = DatabricksGenieInput
    _auth_token: Optional[str] = None
    _databricks_instance: Optional[str] = None
    _genie_space_id: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._databricks_instance = os.getenv("DATABRICKS_INSTANCE")
        self._genie_space_id = os.getenv("GENIE_SPACE_ID")
        self._auth_token = os.getenv("DATABRICKS_TOKEN")
        if not self._auth_token:
            raise ValueError("DATABRICKS_TOKEN environment variable not found.")

        if not self._databricks_instance:
            raise ValueError("DATABRICKS_INSTANCE environment variable not found.")
        if not self._genie_space_id:
            raise ValueError("GENIE_SPACE_ID environment variable not found.")

    def _truncate(self, text: str, max_len: int = 600) -> str:
        if text is None:
            return ""
        return text if len(text) <= max_len else text[: max_len - 3] + "..."

    def _request_ids(self, response) -> str:
        if not response:
            return ""
        rid = (
            response.headers.get("x-request-id")
            or response.headers.get("X-Request-Id")
            or response.headers.get("x-databricks-request-id")
        )
        org = response.headers.get("x-databricks-org-id")
        parts = []
        if rid:
            parts.append(f"request_id={rid}")
        if org:
            parts.append(f"org_id={org}")
        return (" ".join(parts)) if parts else ""

    def _explain_http_error(self, status_code: int, url: str, response_text: str, response=None) -> str:
        hints = []
        if status_code == 401:
            hints.append("Invalid or expired token. Ensure DATABRICKS_TOKEN is a valid Workspace PAT for this instance.")
            hints.append("If you recently rotated the token, restart the app to reload .env.")
        elif status_code == 403:
            hints.append("Forbidden. Your PAT likely lacks permission on the Genie space or workspace.")
            hints.append("Verify: Genie Space → Configure → Permissions; ensure your user has at least 'Can Use'.")
        elif status_code == 404:
            hints.append("Not found. Check GENIE_SPACE_ID and DATABRICKS_INSTANCE host are correct (no protocol in host).")
        elif status_code == 429:
            hints.append("Rate limit. The tool retries with backoff. Consider waiting longer between polls.")
        elif status_code and 500 <= status_code < 600:
            hints.append("Server error. Try again shortly.")

        req_ids = self._request_ids(response)
        base = f"HTTP {status_code} calling {url}"
        details = f"Response body: {self._truncate(response_text)}"
        meta = f"{req_ids}" if req_ids else ""
        hint_text = (" Hints: " + " ".join(hints)) if hints else ""
        return " | ".join([p for p in [base, details, meta] if p]) + hint_text

    def _get_auth_headers(self):
        """Build headers using Personal Access Token (PAT)."""
        return {
            "Authorization": f"Bearer {self._auth_token}",
            "Content-Type": "application/json",
            "User-Agent": "DatabricksGenieNLQTool/1.0"
        }

    def _rate_limited_request(self, method, url, headers, payload=None, max_retries=3, backoff_factor=2):
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = requests.request(method, url, headers=headers, json=payload, timeout=10)
                if response.status_code == 200:
                    return response.json()
                else:
                    # Build a helpful error and raise
                    message = self._explain_http_error(response.status_code, url, response.text, response)
                    http_err = requests.exceptions.HTTPError(message, response=response)
                    raise http_err
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response is not None and getattr(e.response, 'status_code', None) == 429:
                    retry_count += 1
                    wait_time = backoff_factor ** retry_count
                    time.sleep(wait_time)
                else:
                    # Re-raise with enriched message when possible
                    if hasattr(e, 'response') and e.response is not None:
                        status = getattr(e.response, 'status_code', None)
                        msg = self._explain_http_error(status, url, e.response.text, e.response)
                        raise requests.exceptions.HTTPError(msg, response=e.response)
                    raise e
        return None
    
    def _run(
        self,
        natural_language_query: str,
        conversation_id: Optional[str] = None,
        fetch_query_results: bool = False,
        polling_interval_seconds: int = 5,
        polling_timeout_seconds: int = 600,
    ) -> str:
        current_db_instance = self._databricks_instance
        current_genie_space_id = self._genie_space_id

        if "https://" in current_db_instance:
            current_db_instance = current_db_instance.split("https://")[1]
        current_db_instance = current_db_instance.rstrip('/')

        # Build authentication headers (PAT)
        common_headers = self._get_auth_headers()

        message_id = None
        current_conversation_id = conversation_id
        output_parts = []

        if current_conversation_id:
            start_conversation_url = f"https://{current_db_instance}/api/2.0/genie/spaces/{current_genie_space_id}/conversations/{current_conversation_id}/messages"
            payload = {"content": natural_language_query}
            try:
                response_data = self._rate_limited_request("POST", start_conversation_url, headers=common_headers, payload=payload)
                if not response_data:
                    return "Error: Failed to get response when sending follow-up."
                message_id = response_data.get("id")
                if not message_id: # Attempt to find message_id if not at root
                    message_id = response_data.get("message", {}).get("id")
                if not message_id:
                    return f"Error: Could not get message_id from follow-up. Response: {response_data}"
                output_parts.append(f"Follow-up sent. New Message ID: {message_id}")
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response is not None:
                    msg = self._explain_http_error(e.response.status_code, start_conversation_url, e.response.text, e.response)
                    return f"Error sending follow-up: {msg}"
                return f"Error sending follow-up: {str(e)}"
        else:
            start_conversation_url = f"https://{current_db_instance}/api/2.0/genie/spaces/{current_genie_space_id}/start-conversation"
            payload = {"content": natural_language_query}
            try:
                response_data = self._rate_limited_request("POST", start_conversation_url, headers=common_headers, payload=payload)
                if not response_data:
                    return "Error: Failed to get response when starting conversation."
                current_conversation_id = response_data.get("conversation", {}).get("id")
                message_id = response_data.get("message", {}).get("id")
                if not current_conversation_id or not message_id:
                    return f"Error: Missing conversation_id or message_id. Response: {response_data}"
                output_parts.append(f"New conversation. ID: {current_conversation_id}, Message ID: {message_id}")
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response is not None:
                    msg = self._explain_http_error(e.response.status_code, start_conversation_url, e.response.text, e.response)
                    return f"Error starting conversation: {msg}"
                return f"Error starting conversation: {str(e)}"

        if not message_id or not current_conversation_id:
            return "Error: Failed to obtain message or conversation ID."

        poll_url = f"https://{current_db_instance}/api/2.0/genie/spaces/{current_genie_space_id}/conversations/{current_conversation_id}/messages/{message_id}"
        start_time = time.time()
        genie_response_text = "Not available"
        generated_sql = "Not available"
        attachment_id_for_results = None

        while True:
            if time.time() - start_time > polling_timeout_seconds:
                output_parts.append("Error: Polling timed out.")
                return "\n".join(output_parts)

            try:
                poll_data = self._rate_limited_request("GET", poll_url, headers=common_headers)
                if not poll_data:
                    output_parts.append("Error: Failed to get polling response.")
                    time.sleep(polling_interval_seconds)
                    continue
                status = poll_data.get("status")

                if status == "COMPLETED":
                    output_parts.append("Genie processing completed.")
                    attachments = poll_data.get("attachments")
                    
                    if attachments and isinstance(attachments, list) and len(attachments) > 0:
                        for attachment in attachments:
                            current_attachment_text = "Not available"
                            current_attachment_sql = "Not available"

                            if "query" in attachment and isinstance(attachment["query"], dict):
                                query_obj = attachment["query"]
                                if "description" in query_obj:
                                    current_attachment_text = query_obj.get("description", "No description in query object.")
                                if "query" in query_obj:
                                    current_attachment_sql = query_obj.get("query", "No SQL string in query object.")
                            
                            if current_attachment_text == "Not available" and "text" in attachment:
                                raw_text_response = attachment.get("text")
                                if isinstance(raw_text_response, dict) and "content" in raw_text_response:
                                    current_attachment_text = raw_text_response["content"]
                                elif isinstance(raw_text_response, str):
                                    current_attachment_text = raw_text_response
                                else:
                                    current_attachment_text = str(raw_text_response) if raw_text_response is not None else "Text format not recognized."

                            if current_attachment_text != "Not available":
                                genie_response_text = current_attachment_text
                            if current_attachment_sql != "Not available":
                                generated_sql = current_attachment_sql
                            
                            if "attachment_id" in attachment and \
                               (fetch_query_results and \
                                generated_sql not in ["Not available", "No SQL string in query object."]):
                                attachment_id_for_results = attachment.get("attachment_id")
                            
                            if genie_response_text != "Not available" and \
                               generated_sql not in ["Not available", "No SQL string in query object."]:
                                break 
                    else: 
                        raw_content_response = poll_data.get("content")
                        if isinstance(raw_content_response, str) and raw_content_response.lower() != natural_language_query.lower():
                            genie_response_text = raw_content_response
                            output_parts.append("No attachments; used message content for response.")
                        elif isinstance(raw_content_response, dict) and "content" in raw_content_response:
                            genie_response_text = raw_content_response["content"]
                            output_parts.append("No attachments; used message content for response.")
                        else:
                            output_parts.append("No attachments or suitable main content for response.")
                    break 
                elif status in ["FAILED", "CANCELLED"]:
                    error_details = poll_data.get("error", "Unknown error")
                    output_parts.append(f"Error: Genie processing {status}. Details: {error_details}")
                    return "\n".join(output_parts)
                elif status == "IN_PROGRESS":
                    pass 
                else: 
                    output_parts.append(f"Info: Genie API status update - '{status}'.")
                time.sleep(polling_interval_seconds)
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response is not None:
                    msg = self._explain_http_error(e.response.status_code, poll_url, e.response.text, e.response)
                    output_parts.append(f"Error during polling: {msg}")
                else:
                    output_parts.append(f"Error during polling: {str(e)}")
                time.sleep(polling_interval_seconds) 
            except Exception as e: 
                output_parts.append(f"Unexpected error during polling: {str(e)}")
                return "\n".join(output_parts)

        query_results_str = "Not fetched."
        if fetch_query_results and attachment_id_for_results and \
           generated_sql not in ["Not available", "No SQL string in query object."]:
            results_url = f"https://{current_db_instance}/api/2.0/genie/spaces/{current_genie_space_id}/conversations/{current_conversation_id}/messages/{message_id}/attachments/{attachment_id_for_results}/query-result"
            try:
                results_data = self._rate_limited_request("GET", results_url, headers=common_headers)
                if not results_data:
                    query_results_str = "Error: Failed to get query results."
                else:
                    query_results_str = f"Successfully fetched: {str(results_data)}" 
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response is not None:
                    msg = self._explain_http_error(e.response.status_code, results_url, e.response.text, e.response)
                    query_results_str = f"Error fetching results: {msg}"
                else:
                    query_results_str = f"Error fetching results: {str(e)}"
            except Exception as e: 
                query_results_str = f"Error processing results: {str(e)}"
        elif fetch_query_results and \
             generated_sql in ["Not available", "No SQL string in query object."]:
            query_results_str = "Not fetched; no SQL query was generated."
        elif fetch_query_results and not attachment_id_for_results:
            query_results_str = "Not fetched; no attachment_id for results."

        final_output_lines = [
            f"Conversation ID: {current_conversation_id}",
            f"Message ID: {message_id}",
            f"Genie's Textual Response: {genie_response_text}",
            f"Generated SQL Query: {generated_sql}",
            f"Query Results: {query_results_str}"
        ]
        
        # Prepend earlier operational messages if there was an issue but we still got some results
        if any("Error:" in part or "Warning:" in part for part in output_parts) and \
           (genie_response_text != "Not available" or generated_sql != "Not available"):
            final_output_lines = output_parts + ["---"] + final_output_lines
        # If major errors occurred and we got no meaningful response, output_parts would have been returned already.

        return "\n".join(final_output_lines)