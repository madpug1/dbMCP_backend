    from flask import Flask, request, jsonify
    from flask_cors import CORS
    import paramiko
    import json
    import os
    import requests
    import re
    import psycopg2 # Import the PostgreSQL adapter. Change this if you use a different DB.
    from psycopg2 import Error as Psycopg2Error # Specific error handling for psycopg2

    app = Flask(__name__)
    CORS(app)

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @app.route('/api/save-schema', methods=['POST'])
    def save_schema():
        """
        Handles the POST request to save a schema to a JSON file on the SFTP server.
        """
        if not request.is_json:
            print("DEBUG: Request is not JSON in save_schema.")
            return jsonify({"message": "Request must be JSON"}), 400

        data = request.get_json()

        sftp_details = data.get('sftp')
        schema_name = data.get('name')
        fields = data.get("Fields in database table")
        training_sets = data.get('trainingSets')
        llm_endpoint = data.get('llmEndpoint')
        db_credentials = data.get('dbCredentials')

        if not sftp_details or not schema_name or not fields:
            print("DEBUG: Missing SFTP details, schema name, or fields for save_schema")
            return jsonify({"message": "Missing SFTP details, schema name, or fields"}), 400

        host = sftp_details.get('host')
        username = sftp_details.get('username')
        password = sftp_details.get('password')
        port = int(sftp_details.get('port', 22))

        if not host or not username or not password:
            print("DEBUG: SFTP host, username, or password missing for save_schema")
            return jsonify({"message": "SFTP host, username, or password missing"}), 400

        schema_to_save = {
            "schemaName": schema_name,
            "Fields in database table": fields,
            "trainingSets": training_sets,
            "llmEndpoint": llm_endpoint,
            "dbCredentials": db_credentials
        }

        remote_sftp_path = f"/schemas/{schema_name}.json"
        json_content = json.dumps(schema_to_save, indent=4)

        transport = None
        sftp = None
        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            try:
                sftp.stat(remote_sftp_path)
                print(f"DEBUG: Schema '{schema_name}' already exists.")
                return jsonify({"message": f"Error: Schema with the name '{schema_name}' already exists. Please use a different name."}), 409
            except FileNotFoundError:
                pass

            remote_dir = os.path.dirname(remote_sftp_path)
            try:
                sftp.stat(remote_dir)
            except FileNotFoundError:
                print(f"DEBUG: Creating remote SFTP directory: {remote_dir}")
                sftp.mkdir(remote_dir)

            with sftp.open(remote_sftp_path, 'w') as f:
                f.write(json_content)

            print(f"DEBUG: Schema '{schema_name}' uploaded successfully.")
            return jsonify({"message": f"Schema '{schema_name}' uploaded successfully to {remote_sftp_path}"}), 200

        except paramiko.AuthenticationException:
            print("DEBUG: SFTP AuthenticationException in save_schema")
            return jsonify({"message": "SFTP authentication failed. Check username and password."}), 401
        except paramiko.SSHException as e:
            print(f"DEBUG: SSHException in save_schema: {str(e)}")
            return jsonify({"message": f"Could not establish SSH connection: {str(e)}"}), 500
        except Exception as e:
            print(f"DEBUG: Generic Exception in save_schema: {str(e)}")
            return jsonify({"message": f"An error occurred during SFTP transfer: {str(e)}"}), 500
        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @app.route('/api/get-schema', methods=['POST'])
    def get_schema():
        """
        Handles the POST request to retrieve a schema from the SFTP server.
        """
        if not request.is_json:
            print("DEBUG: Request is not JSON in get_schema.")
            return jsonify({"message": "Request must be JSON"}), 400

        data = request.get_json()
        schema_name = data.get('schemaName')
        sftp_details = data.get('sftp')

        if not schema_name or not sftp_details:
            print("DEBUG: Missing schemaName or SFTP details for get_schema")
            return jsonify({"message": "schemaName and SFTP details are required."}), 400

        host = sftp_details.get('host')
        username = sftp_details.get('username')
        password = sftp_details.get('password')
        port = int(sftp_details.get('port', 22))

        if not host or not username or not password:
            print("DEBUG: SFTP host, username, or password missing for get_schema")
            return jsonify({"message": "SFTP host, username, or password missing"}), 400

        remote_sftp_path = f"/schemas/{schema_name}.json"
        local_temp_path = f"{schema_name}_temp.json"

        transport = None
        sftp = None
        try:
            transport = paramiko.Transport((host, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            sftp.get(remote_sftp_path, local_temp_path)
            
            with open(local_temp_path, 'r') as f:
                schema_content = json.load(f)
            
            os.remove(local_temp_path)
            print(f"DEBUG: Schema '{schema_name}' retrieved successfully.")
            return jsonify(schema_content), 200
        
        except paramiko.AuthenticationException:
            print("DEBUG: SFTP AuthenticationException in get_schema")
            return jsonify({"message": "SFTP authentication failed."}), 401
        except FileNotFoundError:
            print(f"DEBUG: Schema '{schema_name}' not found on SFTP server.")
            return jsonify({"message": f"Schema '{schema_name}' not found on the SFTP server."}), 404
        except Exception as e:
            print(f"DEBUG: Generic Exception in get_schema: {str(e)}")
            return jsonify({"message": f"An error occurred: {str(e)}"}), 500
        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @app.route('/api/chat-query', methods=['POST'])
    def chat_query():
        """
        Receives a user query and a schema, then proxies the request to the LLM endpoint.
        It now handles LLM responses that are either prefixed with 'query ->' (for SQL queries) or plain text.
        """
        if not request.is_json:
            print("DEBUG: Request is not JSON in chat_query.")
            return jsonify({"message": "Request must be JSON"}), 400

        data = request.get_json()
        user_query = data.get('query')
        schema = data.get('schema')

        if not user_query or not schema:
            print("DEBUG: Missing query or schema in chat_query.")
            return jsonify({"message": "Query or schema is missing from the request."}), 400

        llm_endpoint_config = schema.get('llmEndpoint')
        db_credentials = schema.get('dbCredentials')

        if not llm_endpoint_config or not llm_endpoint_config.get('url'):
            print("DEBUG: LLM endpoint URL not configured in schema.")
            return jsonify({"message": "LLM endpoint URL is not configured in the schema."}), 400
        
        response_key = llm_endpoint_config.get('body', {}).get('responseKey', '').strip()
        if not response_key:
            print("DEBUG: LLM response key not configured in schema.")
            return jsonify({"message": "LLM response key is not configured in the schema."}), 400

        fields_in_table = schema.get("Fields in database table", "No fields provided.")
        schema_name = schema.get("schemaName", "your database table")
        training_sets = schema.get("trainingSets", [])

        training_examples_str = ""
        if training_sets:
            training_examples_str = "\n".join([
                f"Question: {ts['input']}\nSQL Query: {ts['output']}"
                for ts in training_sets
            ])
        
        # Updated system prompt to instruct LLM for "query -> " prefix
        system_prompt = f"""You are a Database Query Builder for Table {schema_name}.
    These are the fields in the database:
    {fields_in_table}
    """
        if training_examples_str:
            system_prompt += f"""You have the following training examples to guide your query building:
    {training_examples_str}
    """
        system_prompt += """Your job:
    - Convert user questions into valid SQL queries using the schema and training examples.
    - Always return only the SQL query unless the user is just greeting you.
    Special rules:
    - If the user says "hi", "hello", "hey", or similar greetings, respond politely with a short friendly greeting.
    - If the user asks about anything outside SQL queries (e.g., coding, stories, general knowledge), reply only with:
     "I am not trained for this"
    - If the user asked questions related to the database and you are able to create a query, then return your response prefixed with "query -> " followed by the SQL query.
    Example: "query -> SELECT * FROM customers WHERE total_orders > 10;"
    If you cannot make the query (e.g., for greetings, "not trained", or "cannot provide") then return your response as a simple string without any prefix.
    """

        headers = {'Content-Type': 'application/json'}
        if llm_endpoint_config['authType'] == 'Authorization Header':
            headers['Authorization'] = llm_endpoint_config['credentials']['authHeader']
        for header in llm_endpoint_config.get('extraHeaders', []):
            headers[header['key']] = header['value']

        request_body = {}
        
        llm_body_config = llm_endpoint_config.get('body', {})
        sample_json_str = llm_body_config.get('sampleJson', '').strip()
        query_key = llm_body_config.get('queryKey', '').strip()

        if sample_json_str and query_key:
            try:
                request_body = json.loads(sample_json_str)
                set_nested_value(request_body, query_key, system_prompt + user_query)
            except json.JSONDecodeError:
                print("DEBUG: Invalid JSON in LLM request body in chat_query.")
                return jsonify({"message": "Invalid JSON in LLM request body. Please check the sample JSON format."}), 400
            except (IndexError, TypeError) as e:
                print(f"DEBUG: Error with Query Key path in chat_query: {str(e)}")
                if "list" in str(e) and "string" in str(e):
                    return jsonify({"message": f"Error with Query Key path: It seems you're trying to access a list with a string key. Remember to use numeric indices for lists (e.g., 'items.0.name' not 'items.name'). Original error: {str(e)}"}), 400
                else:
                    return jsonify({"message": f"Error with Query Key path: {str(e)}. Please check the Query Key format."}), 400
        else:
            request_body = {'query': system_prompt + user_query}
            
        print("\n--- Payload to LLM ---")
        print(json.dumps(request_body, indent=2))
        print("--- End Payload ---\n")
        
        try:
            response = requests.post(llm_endpoint_config['url'], headers=headers, json=request_body)
            response.raise_for_status()
            
            llm_response_data = response.json()
            print(f"DEBUG: Raw LLM response data: {json.dumps(llm_response_data, indent=2)}")
            
            llm_response_text = get_nested_value(llm_response_data, response_key)
            print(f"DEBUG: Extracted LLM response text: {llm_response_text}")

            # Define the prefix for SQL queries
            SQL_PREFIX = "query -> "

            if llm_response_text.strip().lower().startswith(SQL_PREFIX.lower()):
                # Extract the SQL query by removing the prefix
                sql_query = llm_response_text.strip()[len(SQL_PREFIX):].strip()
                print(f"DEBUG: Successfully extracted SQL from prefixed string: {sql_query}")

                # Validate if it looks like a basic SQL query
                if not re.match(r"^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\s", sql_query, re.IGNORECASE):
                    print(f"DEBUG: LLM response with prefix does not appear to be a SQL query: {sql_query}")
                    return jsonify({"response": "I'm sorry, I couldn't generate a valid SQL query for that request. Please try rephrasing."}), 200

                if not db_credentials:
                    print("DEBUG: Database credentials not provided in schema for query execution.")
                    return jsonify({"message": "Database credentials are not provided in the schema."}), 400
                
                db_query_payload = {
                    "dbCredentials": db_credentials,
                    "query": sql_query
                }
                print(f"DEBUG: Sending to /api/run-query for DB execution: {json.dumps(db_query_payload, indent=2)}")
                
                db_response = requests.post("http://localhost:5000/api/run-query", json=db_query_payload)
                db_response.raise_for_status()
                
                print(f"DEBUG: Received response from /api/run-query: {db_response.json()}")
                return jsonify(db_response.json()), 200
            else:
                # If the response does not start with the SQL_PREFIX, treat it as a plain text message
                print("DEBUG: LLM response does not start with SQL prefix. Treating as plain text.")
                return jsonify({"response": llm_response_text}), 200

        except requests.exceptions.RequestException as e:
            print(f"DEBUG: RequestException in chat_query (LLM or internal DB endpoint): {str(e)}")
            return jsonify({"message": f"Error connecting to LLM or internal DB endpoint: {str(e)}"}), 502
        except Exception as e:
            print(f"DEBUG: Generic Exception in chat_query: {str(e)}")
            return jsonify({"message": f"An unexpected error occurred: {str(e)}"}), 500

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # New Route to Run SQL Query to the Actual Database
    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @app.route('/api/run-query', methods=['POST'])
    def run_query():
        """
        Connects to the database and runs the provided SQL query, returning the results.
        """
        if not request.is_json:
            print("DEBUG: Request is not JSON in run_query.")
            return jsonify({"message": "Request must be JSON"}), 400

        data = request.get_json()
        db_credentials = data.get('dbCredentials')
        query = data.get('query')

        print(f"DEBUG: Received DB query request. Credentials provided: {bool(db_credentials)}, Query provided: {bool(query)}")
        print(f"DEBUG: Query to execute: {query}")

        if not db_credentials or not query:
            print("DEBUG: Missing DB credentials or query for run_query.")
            return jsonify({"message": "Database credentials or query is missing."}), 400

        # Use environment variables for database connection
        db_host = os.environ.get("DB_HOST", db_credentials.get('host'))
        db_user = os.environ.get("DB_USER", db_credentials.get('user'))
        db_password = os.environ.get("DB_PASSWORD", db_credentials.get('password'))
        db_name = os.environ.get("DB_NAME", db_credentials.get('database'))
        db_port = os.environ.get("DB_PORT", db_credentials.get('port')) # Default to 5432 if not set

        if not all([db_host, db_user, db_password, db_name, db_port]):
            print("DEBUG: Incomplete database credentials for run_query.")
            return jsonify({"message": "Incomplete database credentials provided. Ensure DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT are set as environment variables or provided in schema."}), 400

        conn = None
        cursor = None
        try:
            print(f"DEBUG: Attempting to connect to DB: host={db_host}, port={db_port}, user={db_user}, database={db_name}")
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                database=db_name,
                sslmode='require' # Often needed for cloud databases like Render
            )
            cursor = conn.cursor()
            print("DEBUG: Database connection established.")

            cursor.execute(query)
            print("DEBUG: Query executed successfully.")
            
            if cursor.description: # If it's a SELECT query, fetch results.
                results = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                rows = [dict(zip(column_names, row)) for row in results]
                print(f"DEBUG: Fetched {len(rows)} rows from DB.")
                return jsonify({"response": rows}), 200
            else: # For non-SELECT queries (INSERT, UPDATE, DELETE), commit changes and return success
                conn.commit()
                print(f"DEBUG: Non-SELECT query committed. Rows affected: {cursor.rowcount}")
                return jsonify({"response": f"Query executed successfully. Rows affected: {cursor.rowcount}"}), 200

        except Psycopg2Error as e:
            # Ensure e.pgerror is not None before stripping
            pg_error_message = e.pgerror.strip() if e.pgerror else "Unknown database error."
            print(f"DEBUG: Psycopg2Error in run_query: {e.pgcode} - {pg_error_message}")
            # Check if the error is due to a syntax error or a non-SQL command
            if e.pgcode == '42601': # Syntax error
                return jsonify({"message": f"Database query failed: Invalid SQL syntax or non-SQL command. Details: {pg_error_message}"}), 400
            else:
                return jsonify({"message": f"Database error: {e.pgcode} - {pg_error_message}"}), 500
        except Exception as e:
            print(f"DEBUG: Generic Exception in run_query: {str(e)}")
            return jsonify({"message": f"An unexpected error occurred during database operation: {str(e)}"}), 500
        finally:
            if cursor:
                cursor.close()
                print("DEBUG: Cursor closed.")
            if conn:
                conn.close()
                print("DEBUG: DB connection closed.")

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Helper Functions (set_nested_value and get_nested_value remain the same)
    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def set_nested_value(data, keys, value):
        """
        Helper function to set a value in a nested dictionary or list.
        'keys' can be a string like 'messages.0.content' or 'parts[0].text'.
        """
        keys_list = re.findall(r'(\w+)|\[(\d+)\]', keys)
        keys_list = [item for sublist in keys_list for item in sublist if item]
        
        current_data = data
        for i, key_part in enumerate(keys_list[:-1]):
            if key_part.isdigit():
                key_val = int(key_part)
                if not isinstance(current_data, list):
                    raise TypeError(f"Path segment '{key_part}' (index) is used on a non-list type '{type(current_data).__name__}'. Full path: '{keys}'")
                if key_val >= len(current_data):
                    raise IndexError(f"List index '{key_val}' out of range. Full path: '{keys}'")
                current_data = current_data[key_val]
            else:
                if not isinstance(current_data, dict):
                    raise TypeError(f"Path segment '{key_part}' (key) is used on a non-dict type '{type(current_data).__name__}'. Full path: '{keys}'")
                current_data = current_data[key_part]
        
        final_key_part = keys_list[-1]
        if final_key_part.isdigit():
            final_key_val = int(final_key_part)
            if not isinstance(current_data, list):
                raise TypeError(f"Final path segment '{final_key_part}' (index) is used on a non-list type '{type(current_data).__name__}'. Full path: '{keys}'")
            if final_key_val >= len(current_data):
                raise IndexError(f"List index '{final_key_val}' out of range. Full path: '{keys}'")
            current_data[final_key_val] = value
        else:
            if not isinstance(current_data, dict):
                raise TypeError(f"Final path segment '{final_key_part}' (key) is used on a non-dict type '{type(current_data).__name__}'. Full path: '{keys}'")
            current_data[final_key_part] = value

    def get_nested_value(data, keys):
        """
        Helper function to get a value from a nested dictionary or list.
        'keys' can be a string like 'candidates.0.content.parts.0.text'.
        """
        keys_list = re.findall(r'(\w+)|\[(\d+)\]', keys)
        keys_list = [item for sublist in keys_list for item in sublist if item]

        current_data = data
        for key_part in keys_list:
            if isinstance(current_data, dict):
                current_data = current_data.get(key_part)
            elif isinstance(current_data, list):
                try:
                    current_data = current_data[int(key_part)]
                except (ValueError, IndexError, TypeError):
                    current_data = None
            else:
                current_data = None
            
            if current_data is None:
                return 'No response content found.'

        return current_data

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @app.route('/')
    def home():
        """
        Basic route to confirm the Flask backend is running.
        """
        return "Python Backend is Running!"

    #------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    if __name__ == '__main__':
        # Use environment variable for port, default to 5000 for local dev
        port = int(os.environ.get("PORT", 5000))
        app.run(host='0.0.0.0', port=port, debug=False) # Set debug=False for production
    
