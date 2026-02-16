import requests
from typing import Optional

#! CLASS ONLY
# Cannot be ran solo

class GoogleSheetsPayload:
    """Send player stat payloads to Google Apps Script."""
    
    def __init__(self, payload: str):
        """
        Initialize with a payload to send to Google Apps Script.
        
        Args:
            payload: String formatted as "playerName statName statValue"
            script_url: The deployment URL of your Google Apps Script doPost endpoint.
                       Can also be set via the script_url property.
        """
        GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzdH8_UPQpcQ2Wc2fT6FR7uXAoY2GibvgywnhBllG6RtVB_JZby20GItK9lBOnRi3qh/exec"

        self.payload = payload
        self.script_url = GOOGLE_SCRIPT_URL
    
    def send(self) -> requests.Response:
        """
        Send the payload to the Google Apps Script.
        
        Returns:
            Response object from the POST request
            
        Raises:
            ValueError: If script_url is not set
            requests.exceptions.RequestException: If the request fails
        """
        if not self.script_url:
            raise ValueError("script_url must be set before sending")
        
        headers = {'Content-Type': 'text/plain;charset=utf-8'}
        response = requests.post(self.script_url, data=self.payload, headers=headers)
        response.raise_for_status()
        return response
    
    def send_to_url(self, script_url: str) -> requests.Response:
        """
        Send the payload to a specific Google Apps Script URL.
        
        Args:
            script_url: The deployment URL of your Google Apps Script doPost endpoint
            
        Returns:
            Response object from the POST request
        """
        headers = {'Content-Type': 'text/plain;charset=utf-8'}
        response = requests.post(script_url, data=self.payload, headers=headers)
        response.raise_for_status()
        return response

