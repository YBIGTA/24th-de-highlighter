import pandas as pd
from datetime import timedelta

# Read the CSV file into a DataFrame
file_path = 'data/parischim.csv'
df = pd.read_csv(file_path, header=None, names=['time', 'text'])

# Function to normalize time format to hh:mm:ss
def normalize_time_format(time_str):
    parts = time_str.split(':')
    
    if len(parts) == 2:
        # Format is mm:ss, convert to hh:mm:ss
        minutes, seconds = parts
        return f"00:{minutes.zfill(2)}:{seconds.zfill(2)}"
    elif len(parts) == 3:
        # Check if it's mm:ss:00 or hh:mm:ss
        if len(parts[0]) == 1:
            # Format is mm:ss:00, convert to hh:mm:ss
            minutes, seconds, _ = parts
            return f"00:{minutes.zfill(2)}:{seconds.zfill(2)}"
        else:
            # Format is hh:mm:ss, already correct
            hours, minutes, seconds = parts
            return f"{hours.zfill(2)}:{minutes.zfill(2)}:{seconds.zfill(2)}"
    else:
        raise ValueError(f"Unexpected time format: {time_str}")

# Normalize the time column
df['normalized_time'] = df['time'].apply(normalize_time_format)

# Display the corrected DataFrame
print(df[['normalized_time', 'text']])

# Optionally, save the corrected DataFrame back to a CSV file
df.to_csv('data/normalized_parischim.csv', columns=['normalized_time', 'text'], index=False, header=False)