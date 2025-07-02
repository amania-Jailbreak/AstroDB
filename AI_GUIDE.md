# AI Development Guide for AstroDB

This document outlines guidelines and considerations for AI agents working with the AstroDB project.

## 1. Project Overview

AstroDB is a lightweight, in-memory key-value store with persistence, authentication, and query capabilities. It's designed for simplicity and performance.

## 2. Codebase Structure

*   `main.py`: Entry point for the server.
*   `server.py`: Core server logic, handling connections and command parsing.
*   `client.py`: Example client for interacting with the server.
*   `auth_engine.py`: Handles user authentication and authorization.
*   `query_engine.py`: Manages data storage and retrieval, and query operations.
*   `encryption.py`: Provides encryption utilities.
*   `persistence_test.py`, `persistence_verify.py`: Scripts for testing persistence.
*   `benchmark.py`: Performance benchmarking script.
*   `automation_engine.py`: (If applicable) Contains logic for automated tasks or integrations.
*   `run_all_tests.py`: Script to execute all tests.
*   `test.py`: Contains unit tests.

## 3. Development Guidelines for AI Agents

### A. Adherence to Existing Patterns

*   **Code Style:** Mimic the existing Python code style (e.g., variable naming, function definitions, docstrings).
*   **Error Handling:** Follow existing error handling patterns (e.g., exceptions, logging).
*   **Modularity:** Maintain the modular structure of the project. Avoid creating monolithic functions or classes.

### B. Testing

*   **Write Tests:** For any new features or bug fixes, write corresponding unit tests in `test.py` or a new dedicated test file if the scope is large.
*   **Run Tests:** Always run `python3 run_all_tests.py` after making changes to ensure no regressions are introduced.

### C. Security Considerations

*   **Authentication/Authorization:** When modifying authentication or authorization logic, be extremely careful. Ensure no vulnerabilities are introduced.
*   **Data Handling:** Be mindful of how sensitive data is handled, especially regarding encryption and persistence.

### D. Performance

*   **Efficiency:** Consider the performance implications of your changes, especially in `query_engine.py` and `server.py`.
*   **Benchmarking:** If making significant changes to core data operations, consider running `benchmark.py` before and after your changes.

### E. Documentation

*   **Internal Comments:** Add comments for complex logic or non-obvious design choices.
*   **Usage Guide:** If your changes affect how users interact with AstroDB, update `wiki/Usage.md`.

## 4. Common Tasks for AI Agents

*   **Bug Fixing:** Identify and fix bugs, ensuring test coverage.
*   **Feature Implementation:** Add new features, adhering to the project's architecture.
*   **Refactoring:** Improve code readability, maintainability, and performance without changing external behavior.
*   **Testing:** Expand test coverage, create new test cases.
*   **Documentation:** Update existing documentation or create new guides.

---

**Important:** Always confirm your understanding of a task and its implications before making significant changes. When in doubt, ask for clarification.
