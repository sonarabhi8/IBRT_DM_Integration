# SQL to Blob Storage Azure Function

This Azure Function extracts data from SQL Server and exports it to Azure Blob Storage as CSV.

## Configuration

Update the SQL Server credentials in `local.settings.json`:
- Replace `your_username` and `your_password` with your SQL Server credentials

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install ODBC Driver 17 for SQL Server (if not already installed):
   - Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

## Running Locally

```bash
func start
```

## Testing

Trigger the function via HTTP request:
```bash
curl http://localhost:7071/api/export
```

## Deployment

Deploy to Azure:
```bash
func azure functionapp publish <your-function-app-name>
```

Remember to configure the environment variables in Azure Portal after deployment.
