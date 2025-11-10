import pandas as pd
import csv
import io

# --- Helper Functions ---

def wrap_key_or_value(val_str):
    """Wraps a simple string in the 'VString' JSON format."""
    if pd.isna(val_str):
        return ""
    val_str = str(val_str).replace('"', '\\"')
    return f'{{"type":"VString","value":"{val_str}"}}'

def wrap_get_output(output_str):
    """Wraps a GET operation's output in the 'VOption' JSON format."""
    if pd.isna(output_str) or output_str == 'NotFound':
        return '{"type":"VOption","value":null}'
    else:
        wrapped_val = wrap_key_or_value(output_str)
        return f'{{"type":"VOption","value":{wrapped_val}}}'

def get_op_output():
    """Returns the expected output for a successful PUT or DELETE."""
    # We assume DELETE, like PUT, returns an "OK" tuple.
    return '{"type":"VTuple","value":[]}'

# --- Main Conversion Logic ---

# 1. Define the input CSV data from your prompt
csv_data = """call_ns,return_ns,client_id,op,key,value,output
1000,5000,0,PUT,k1,apple,
1500,4500,2,PUT,k2,banana,
2000,3000,1,GET,k1,,NotFound
2000,7000,10,PUT,k4,egg,
3000,8000,8,PUT,k3,dog,
4600,5200,3,GET,k2,,banana
4700,6000,6,DELETE,k2,,
5100,9000,4,PUT,k1,cherry,
6100,6500,7,GET,k2,,NotFound
7100,7200,11,GET,k4,,egg
8100,8300,9,GET,k3,,dog
9200,10000,5,GET,k1,,cherry
10050,10500,0,GET,k1,,cherry
6600,7300,1,PUT,k2,fig,
7400,7600,3,GET,k2,,fig
9050,9300,2,GET,k1,,cherry
11000,12000,4,DELETE,k1,,
12100,12500,5,GET,k1,,NotFound
12600,13000,6,PUT,k5,ham,
13100,13300,7,GET,k5,,ham
8400,9500,8,PUT,k3,iguana,
9600,9800,9,GET,k3,,iguana
7250,7400,10,DELETE,k4,,
7450,7600,11,GET,k4,,NotFound
"""

# 2. Save the data to a file (so we can read it with pandas)
input_filename = "examples/oldkv/sync-1.csv"
with open(input_filename, "w") as f:
    f.write(csv_data)

try:
    # 3. Load the input CSV
    df = pd.read_csv(input_filename)
    df.columns = [col.strip().lower() for col in df.columns]

    print(f"--- Loaded {input_filename} ---")
    print(f"Found {len(df)} operations.")

    # 4. Iterate and transform
    new_rows = []
    target_headers = ["UniqueID", "ClientID", "Kind", "Action", "Payload1", "Payload2", "Payload3"]

    # Use row index as the UniqueID
    for i, row in df.iterrows():
        uid = i
        client_id = row['client_id']
        op = str(row['op']).upper().strip()

        action, p1_inv, p2_inv, p3_inv = "", "", "", ""
        p1_resp, p2_resp, p3_resp = "", "", ""

        if op == 'PUT':
            action = "32_write"
            p2_inv = wrap_key_or_value(row['key'])
            p3_inv = wrap_key_or_value(row['value'])
            p1_resp = get_op_output()

        elif op == 'GET':
            action = "33_read"
            p2_inv = wrap_key_or_value(row['key'])
            p1_resp = wrap_get_output(row['output'])

        elif op == 'DELETE':
            action = "34_delete" # Assigning a new op code
            p2_inv = wrap_key_or_value(row['key'])
            p1_resp = get_op_output() # Assuming DELETE returns "OK"

        else:
            print(f"Warning: Skipping unknown operation '{op}' at row {i}")
            continue

        # Add the Invocation row
        new_rows.append([uid, client_id, "Invocation", action, p1_inv, p2_inv, p3_inv])
        # Add the Response row
        new_rows.append([uid, client_id, "Response", action, p1_resp, p2_resp, p3_resp])

    # 5. Create new DataFrame and save to CSV
    converted_df = pd.DataFrame(new_rows, columns=target_headers)
    output_filename = "complex_history_converted.csv"

    # Use QUOTE_MINIMAL to avoid unnecessary quotes
    converted_df.to_csv(output_filename, index=False, quoting=csv.QUOTE_MINIMAL)

    print(f"\n--- Conversion Successful ---")
    print(f"Converted {len(df)} operations into {len(converted_df)} event rows.")
    print(f"Saved to: {output_filename}")

except Exception as e:
    print(f"An error occurred: {e}")