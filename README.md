# @lishenxydlgzs/aws-athena-mcp
[![smithery badge](https://smithery.ai/badge/@lishenxydlgzs/aws-athena-mcp)](https://smithery.ai/server/@lishenxydlgzs/aws-athena-mcp)

A Model Context Protocol (MCP) server for running AWS Athena queries. This server enables AI assistants to execute SQL queries against your AWS Athena databases and retrieve results.

<a href="https://glama.ai/mcp/servers/0i7dhkex6t">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/0i7dhkex6t/badge" alt="aws-athena-mcp MCP server" />
</a>

## Usage

### Transport Modes

This MCP server supports two transport modes:

1. **stdio** (default): For local desktop applications like Claude Desktop
2. **sse**: For web applications and remote access via HTTP Server-Sent Events

### stdio Mode (Local Desktop Applications)

1. Configure AWS credentials using one of the following methods:
   - AWS CLI configuration
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
   - IAM role (if running on AWS)

2. Add the server to your MCP configuration:

```json
{
  "mcpServers": {
    "athena": {
      "command": "npx",
      "args": ["-y", "@lishenxydlgzs/aws-athena-mcp"],
      "env": {
        // Required
        "OUTPUT_S3_PATH": "s3://your-bucket/athena-results/",
        
        // Optional AWS configuration
        "AWS_REGION": "us-east-1",                    // Default: AWS CLI default region
        "AWS_PROFILE": "default",                     // Default: 'default' profile
        "AWS_ACCESS_KEY_ID": "",                      // Optional: AWS access key
        "AWS_SECRET_ACCESS_KEY": "",                  // Optional: AWS secret key
        "AWS_SESSION_TOKEN": "",                      // Optional: AWS session token
        
        // Optional server configuration
        "ATHENA_WORKGROUP": "default_workgroup",      // Optional: specify the Athena WorkGroup
        "QUERY_TIMEOUT_MS": "300000",                 // Default: 5 minutes (300000ms)
        "MAX_RETRIES": "100",                         // Default: 100 attempts
        "RETRY_DELAY_MS": "500"                       // Default: 500ms between retries
      }
    }
  }
}
```

### SSE Mode (Web Applications & Remote Access)

For web applications or remote access, you can run the server in SSE (Server-Sent Events) mode:

1. Set the `TRANSPORT_MODE` environment variable to `sse`
2. Optionally set the `PORT` (default: 3000)

**Running with Node.js:**

```bash
# Set environment variables
export TRANSPORT_MODE=sse
export PORT=3000
export OUTPUT_S3_PATH=s3://your-bucket/athena-results/
export AWS_REGION=us-east-1

# Run the server
npx -y @lishenxydlgzs/aws-athena-mcp
```

**Running with Docker:**

```dockerfile
FROM node:20-alpine

WORKDIR /app

# Install the package globally
RUN npm install -g @lishenxydlgzs/aws-athena-mcp

# Set environment variables
ENV TRANSPORT_MODE=sse
ENV PORT=3000
ENV OUTPUT_S3_PATH=s3://your-bucket/athena-results/
ENV AWS_REGION=us-east-1

EXPOSE 3000

CMD ["aws-athena-mcp"]
```

**SSE Endpoints:**

- `GET /sse` - SSE connection endpoint for MCP protocol
- `POST /message` - Message endpoint for client requests
- `GET /health` - Health check endpoint

**Example MCP Client Configuration for SSE:**

```json
{
  "mcpServers": {
    "athena-sse": {
      "url": "http://localhost:3000/sse",
      "transport": "sse"
    }
  }
}
```

**Testing the SSE Server:**

A test client is included in `test-client.html`. To use it:

1. Start the server in SSE mode
2. Open `test-client.html` in your browser
3. Click "Connect" to establish SSE connection
4. Use "Health Check" to verify server status
5. Enter a database and query, then click "Run Query"

---

3. The server provides the following tools:

- `run_query`: Execute a SQL query using AWS Athena
  - Parameters:
    - database: The Athena database to query
    - query: SQL query to execute
    - maxRows: Maximum number of rows to return (default: 1000, max: 10000)
  - Returns:
    - If query completes within timeout: Full query results
    - If timeout reached: Only the queryExecutionId for later retrieval

- `get_status`: Check the status of a query execution
  - Parameters:
    - queryExecutionId: The ID returned from run_query
  - Returns:
    - state: Query state (QUEUED, RUNNING, SUCCEEDED, FAILED, or CANCELLED)
    - stateChangeReason: Reason for state change (if any)
    - submissionDateTime: When the query was submitted
    - completionDateTime: When the query completed (if finished)
    - statistics: Query execution statistics (if available)

- `get_result`: Retrieve results for a completed query
  - Parameters:
    - queryExecutionId: The ID returned from run_query
    - maxRows: Maximum number of rows to return (default: 1000, max: 10000)
  - Returns:
    - Full query results if the query has completed successfully
    - Error if query failed or is still running

- `list_saved_queries`: List all saved (named) queries in Athena.

- Returns:
  - An array of saved queries with `id`, `name`, and optional `description`
  - Queries are returned from the configured `ATHENA_WORKGROUP` and `AWS_REGION`

- run_saved_query: Run a previously saved query by its ID.
- Parameters:
  - `namedQueryId`: ID of the saved query
  - `databaseOverride`: Optional override of the saved query's default database
  - `maxRows`: Maximum number of rows to return (default: 1000)
  - `timeoutMs`: Timeout in milliseconds (default: 60000)
- Returns:
  - Same behavior as `run_query`: full results or execution ID

---

## Usage Examples

### Show All Databases
Message to AI Assistant:
```List all databases in Athena```

MCP parameter:
```json
{
  "database": "default",
  "query": "SHOW DATABASES"
}
```

### List Tables in a Database
Message to AI Assistant:
```Show me all tables in the default database```

MCP parameter:
```json
{
  "database": "default",
  "query": "SHOW TABLES"
}
```

### Get Table Schema
Message to AI Assistant:
```What's the schema of the asin_sitebestimg table?```

MCP parameter:
```json
{
  "database": "default",
  "query": "DESCRIBE default.asin_sitebestimg"
}
```

### Table Rows Preview
Message to AI Assistant:
```Show some rows from my_database.mytable```

MCP parameter:
```json
{
  "database": "my_database",
  "query": "SELECT * FROM my_table LIMIT 10",
  "maxRows": 10
}
```

### Advanced Query with Filtering and Aggregation
Message to AI Assistant:
```Find the average price by category for in-stock products```

MCP parameter:
```json
{
  "database": "my_database",
  "query": "SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM products WHERE in_stock = true GROUP BY category ORDER BY count DESC",
  "maxRows": 100
}
```

### Checking Query Status
```json
{
  "queryExecutionId": "12345-67890-abcdef"
}
```

### Getting Results for a Completed Query
```json
{
  "queryExecutionId": "12345-67890-abcdef",
  "maxRows": 10
}
```

### Listing Saved Queries
```json
{
  "name": "list_saved_queries",
  "arguments": {}
}
```

### Running a Saved Query
```json
{
  "name": "run_saved_query",
  "arguments": {
    "namedQueryId": "abcd-1234-efgh-5678",
    "maxRows": 100
  }
}
```

---

## Deployment

### Docker Deployment

#### Using Docker Compose (Recommended)

1. Create a `.env` file with your configuration:

```bash
OUTPUT_S3_PATH=s3://your-bucket/athena-results/
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
ATHENA_WORKGROUP=primary
```

2. Run with Docker Compose:

```bash
docker-compose up -d
```

3. Check the logs:

```bash
docker-compose logs -f
```

4. Access the server at `http://localhost:3000`

#### Using Dockerfile

1. Build the image:

```bash
docker build -t aws-athena-mcp:latest .
```

2. Run the container:

```bash
docker run -d \
  --name aws-athena-mcp \
  -p 3000:3000 \
  -e TRANSPORT_MODE=sse \
  -e OUTPUT_S3_PATH=s3://your-bucket/athena-results/ \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=your-access-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret-key \
  -v ~/.aws:/root/.aws:ro \
  aws-athena-mcp:latest
```

3. Check health:

```bash
curl http://localhost:3000/health
```

### Production Deployment Considerations

When deploying in SSE mode for production:

1. **Security**:
   - Use HTTPS with a reverse proxy (nginx, Caddy, etc.)
   - Implement authentication/authorization
   - Restrict CORS origins in production
   - Use IAM roles instead of access keys when possible

2. **Scalability**:
   - Use a load balancer for multiple instances
   - Consider using AWS ECS/EKS for container orchestration
   - Monitor resource usage and scale accordingly

3. **Monitoring**:
   - Set up health check monitoring
   - Log aggregation (CloudWatch, ELK, etc.)
   - Set up alerts for failures

---

## Requirements

- Node.js >= 16
- AWS credentials with appropriate Athena and S3 permissions
- S3 bucket for query results
- Named queries (optional) must exist in the specified `ATHENA_WORKGROUP` and `AWS_REGION`

---

## License

MIT

## Repository

[GitHub Repository](https://github.com/lishenxydlgzs/aws-athena-mcp)
