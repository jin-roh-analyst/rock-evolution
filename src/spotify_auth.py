#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  7 21:21:37 2025

@author: jin
"""
import os
import requests
from dotenv import load_dotenv

# Load secrets from your .env file
load_dotenv()

CID = os.getenv("SPOTIFY_CLIENT_ID")
CS = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_token():
    """
    Get a Spotify API token using Client Credentials Flow.
    """
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data, auth=(CID, CS))
    response.raise_for_status()  # raise error if something goes wrong

    token_info = response.json()
    return token_info["access_token"]

if __name__ == "__main__":
    token = get_token()
    print("Your access token is:\n", token[:80] + "...")
