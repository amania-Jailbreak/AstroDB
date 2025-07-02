# AstroDB WebSocket API Reference

This document provides a reference for interacting with the AstroDB server via WebSocket. The API is command-based, where clients send JSON objects representing commands and receive JSON responses.

## Base URL

`ws://127.0.0.1:8000/ws`

## Authentication

All data manipulation commands require authentication. First, register a user, then log in to obtain an authentication token. This token must be included in subsequent authenticated requests.

### REGISTER

Registers a new user.

*   **Command:** `REGISTER`
*   **Request Parameters:**
    *   `username` (string, required): The desired username.
    *   `password` (string, required): The desired password.
*   **Example Request:**
    ```json
    {
        "command": "REGISTER",
        "username": "myuser",
        "password": "mypassword"
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "message": "User registration successful."
    }
    ```
*   **Error Response:**
    ```json
    {
        "status": "error",
        "message": "User already exists."
    }
    ```

### LOGIN

Authenticates a user and returns an access token.

*   **Command:** `LOGIN`
*   **Request Parameters:**
    *   `username` (string, required): The user's username.
    *   `password` (string, required): The user's password.
*   **Example Request:**
    ```json
    {
        "command": "LOGIN",
        "username": "myuser",
        "password": "mypassword"
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
    }
    ```
*   **Error Response:**
    ```json
    {
        "status": "error",
        "message": "Invalid username or password."
    }
    ```

## Authenticated Commands

For all commands below, the request JSON **must** include a `token` field with a valid JWT obtained from the `LOGIN` command.

### INSERT_ONE

Inserts a single document into a specified collection.

*   **Command:** `INSERT_ONE`
*   **Request Parameters:**
    *   `token` (string, required): Authentication token.
    *   `collection` (string, required): The name of the collection.
    *   `document` (object, required): The document to insert. An `_id` will be automatically generated if not provided. `owner_id` will be set to the authenticated user's ID.
*   **Example Request:**
    ```json
    {
        "command": "INSERT_ONE",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "collection": "users",
        "document": {
            "name": "Alice",
            "age": 30
        }
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "data": {
            "name": "Alice",
            "age": 30,
            "_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
            "owner_id": "myuser"
        }
    }
    ```
*   **Error Responses:**
    *   `Collection must be a non-empty string.`
    *   `Document must be a non-empty dictionary.`
    *   `Cannot insert documents with another user's owner_id.`
    *   `Authentication token is required.`
    *   `Invalid or expired token.`

### FIND

Finds documents in a specified collection that match a given query.

*   **Command:** `FIND`
*   **Request Parameters:**
    *   `token` (string, required): Authentication token.
    *   `collection` (string, required): The name of the collection.
    *   `query` (object, optional): A dictionary specifying the query criteria. If omitted, all documents in the collection (owned by the user) are returned. Supports nested fields (dot notation) and advanced operators (`$regex`, `$size`, `$exists`, etc.).
*   **Example Request (find all):**
    ```json
    {
        "command": "FIND",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "collection": "users",
        "query": {}
    }
    ```
*   **Example Request (find by name):**
    ```json
    {
        "command": "FIND",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "collection": "users",
        "query": {
            "name": "Alice"
        }
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "data": [
            {
                "name": "Alice",
                "age": 30,
                "_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
                "owner_id": "myuser"
            },
            {
                "name": "Bob",
                "age": 25,
                "_id": "b1c2d3e4-f5a6-7890-1234-567890abcdef",
                "owner_id": "myuser"
            }
        ]
    }
    ```
*   **Error Responses:**
    *   `Collection must be a non-empty string.`
    *   `Query must be a dictionary.`
    *   `Authentication token is required.`
    *   `Invalid or expired token.`

### UPDATE_ONE

Updates a single document in a specified collection that matches the query.

*   **Command:** `UPDATE_ONE`
*   **Request Parameters:**
    *   `token` (string, required): Authentication token.
    *   `collection` (string, required): The name of the collection.
    *   `query` (object, required): The query criteria to find the document to update.
    *   `update_data` (object, required): A dictionary containing the fields and new values to update. `_id` and `owner_id` cannot be updated.
*   **Example Request:**
    ```json
    {
        "command": "UPDATE_ONE",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "collection": "users",
        "query": {
            "name": "Alice"
        },
        "update_data": {
            "age": 31,
            "status": "active"
        }
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "data": {
            "name": "Alice",
            "age": 31,
            "status": "active",
            "_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
            "owner_id": "myuser"
        }
    }
    ```
*   **Error Responses:**
    *   `Collection must be a non-empty string.`
    *   `Query must be a dictionary.`
    *   `Update data must be a non-empty dictionary.`
    *   `Document to update not found or insufficient permissions.`
    *   `Authentication token is required.`
    *   `Invalid or expired token.`

### DELETE_ONE

Deletes a single document from a specified collection that matches the query.

*   **Command:** `DELETE_ONE`
*   **Request Parameters:**
    *   `token` (string, required): Authentication token.
    *   `collection` (string, required): The name of the collection.
    *   `query` (object, required): The query criteria to find the document to delete.
*   **Example Request:**
    ```json
    {
        "command": "DELETE_ONE",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "collection": "users",
        "query": {
            "name": "Alice"
        }
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "data": {
            "name": "Alice",
            "age": 31,
            "status": "active",
            "_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
            "owner_id": "myuser"
        }
    }
    ```
*   **Error Responses:**
    *   `Collection must be a non-empty string.`
    *   `Query must be a dictionary.`
    *   `Document to delete not found or insufficient permissions.`
    *   `Authentication token is required.`
    *   `Invalid or expired token.`

### CREATE_INDEX

Creates an index on a specified field within a collection.

*   **Command:** `CREATE_INDEX`
*   **Request Parameters:**
    *   `token` (string, required): Authentication token.
    *   `collection` (string, required): The name of the collection.
    *   `field` (string, required): The name of the field to index.
*   **Example Request:**
    ```json
    {
        "command": "CREATE_INDEX",
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "collection": "users",
        "field": "email"
    }
    ```
*   **Success Response:**
    ```json
    {
        "status": "ok",
        "message": "Index created on collection 'users', field 'email'."
    }
    ```
*   **Error Responses:**
    *   `Collection must be a non-empty string.`
    *   `Field must be a non-empty string.`
    *   `Authentication token is required.`
    *   `Invalid or expired token.`
