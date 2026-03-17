# Impact Insights - Productrion Deployment Guide

Welcome to the **Impact Insights** project! This tool helps in analyzing the impact of code changes for Ackumen, allowing you to assess the ripple effects of various modifications.

## Pre-requisites

Before using the **Impact Insights**, please make sure you have the following installed and configured:

### 1. Install SQLite3

  ```bash
  sudo apt install sqlite3
  ```

### 2. Install Flask


  ```bash
  sudo apt install python3-flask
  ```

### 3. Install Gunicorn as Production WSGI


  ```bash
  sudo apt install gunicorn
  ```

### 4. Install NGINX for reverse proxy


  ```bash
  sudo apt install nginx
  ```

### 5. Set PYTHONPATH

  ```bash
  export PYTHONPATH=$PTYHONPATH:./
  ```

## Setting-up Collectors

Collectors are small snippets of code that are responsible for collecting information for ADO and building the Knowledge Base.

Set the cron jobs as below:

  ```bash
  0 */4 * * * cd ~/Insights4 && make collect
  ```


## Deploying the Web Application

Deploy the web application as a system service using the impactinsights.service file. Modify the file appropriately.

  ```bash
  sudo cp impactinsights.service /etc/systemd/system
  sudo systemctl daemon-reload
  sudo systemctl enable impactinsights.service
  sudo systemctl start impactinsights.service
  ```

## Setup Reverse Proxy

To setup reverse proxy for the Gunicorn application, modift the impactinsights_reverse_proxy file and follow the below steps

  ```bash
  sudo cp impactinsights_reverse_proxy /etc/nginx/sites-available/
  sudo ln -s /etc/nginx/sites-available/impactinsights_reverse_proxy /etc/nginx/sites-enabled/
  # Check if there is no error in nginx configuration
  sudo nginx -t
  sudo systemctl restart nginx
  ```
