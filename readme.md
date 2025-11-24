# Personal Project: Implementation of the SES Algorithm

## 1. Description

This program illustrates the implementation of the SES (Schiper–Eggli–Sandoz) algorithm.

Includes 15 processes
Each process automatically sends 150 messages to every other process.
Message content can be simple, such as: "message 1", "message 2", ...
Message generation time is random.

## 2. Program Design

The program adheres to the SES algorithm. It maintains a vector V_P to decide whether to deliver the message or buffer the message.

## 3. Project Directory Structure

```txt
.
├── config/
├── logs/
├── readme.md
├── requirements.txt
├── single_process.py
├── launcher.py
└── temp_status/
```

## 4. How to Run the Program
Environment requirement: Python 3.10+
### Configuration
Parameters are stored in `config.json`
```json
{
    "num_processes": 15,
    "messages_per_process": 150,
    "base_port": 5000,
    "processes": [
        {"id": 0, "host": "localhost", "port": 5000},
        {"id": 1, "host": "localhost", "port": 5001}
        ...
    ]
}
```
### Running the Program:
```python 
python launcher.py
```
The launcher will automatically follow the config.json configuration:
+ Initialize all processes
+ Save logs to logs/PX.log for each process