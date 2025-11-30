# Python Data Engineering Patterns Guide

A comprehensive collection of common Python patterns for data engineering tasks.

---

## Table of Contents

1. [Data Deduplication](#data-deduplication)
2. [Large File Processing](#large-file-processing)
3. [Data Compression](#data-compression)
4. [JSON Processing](#json-processing)
5. [Statistical Analysis](#statistical-analysis)
6. [Algorithm Patterns](#algorithm-patterns)
7. [ETL Pipelines](#etl-pipelines)

---

## Data Deduplication

### Deduplicate List of Dictionaries

**Method 1: Using Set with Tuple Conversion**

Best for: Complete row deduplication across all fields.

```python
input_data = [
    {'id': 1, 'name': 'Alice', 'city': 'Paris'},
    {'id': 1, 'name': 'Alice', 'city': 'Paris'},  # Duplicate
    {'id': 2, 'name': 'Bob', 'city': 'London'},
    {'id': 3, 'name': 'Charlie', 'city': 'Berlin'},
    {'id': 2, 'name': 'Bob', 'city': 'London'}   # Duplicate
]

# Convert each dict to hashable tuple for deduplication
seen = set()
output = []

for row in input_data:
    key = tuple(row.items())  # Convert dict to hashable tuple
    if key not in seen:
        seen.add(key)
        output.append(row)

print(output)
# Output: [{'id': 1, 'name': 'Alice', 'city': 'Paris'}, 
#          {'id': 2, 'name': 'Bob', 'city': 'London'}, 
#          {'id': 3, 'name': 'Charlie', 'city': 'Berlin'}]
```

**Method 2: Using Dictionary with ID as Key**

Best for: Deduplication by specific key field (keeps last occurrence).

```python
unique = {}
for row in input_data:
    unique[row["id"]] = row  # Last occurrence wins

output = list(unique.values())
print(output)
```

**Method 3: Keep First Occurrence by ID**

```python
unique = {}
for row in input_data:
    # Use setdefault to keep first occurrence
    unique.setdefault(row["id"], row)

output = list(unique.values())
```

**Method 4: Using Pandas (For Large Datasets)**

```python
import pandas as pd

df = pd.DataFrame(input_data)

# Method 1: Drop duplicates based on all columns
df_dedup = df.drop_duplicates()

# Method 2: Drop duplicates based on specific column(s)
df_dedup_id = df.drop_duplicates(subset=['id'], keep='first')

# Method 3: Drop duplicates, keep last occurrence
df_dedup_last = df.drop_duplicates(subset=['id'], keep='last')
```

---

## Large File Processing

### Parse Large CSV in Chunks

Process massive CSV files without loading entire dataset into memory.

```python
import pandas as pd
from io import StringIO

# Mock CSV data (small example for demonstration)
csv_data = """id,name,age,city
1,Alice,30,Paris
2,Bob,25,London
3,Charlie,35,Berlin
4,Diana,28,Rome
5,Eve,32,Madrid
6,Frank,29,Amsterdam
7,Grace,31,Brussels
8,Henry,27,Vienna
"""

# Simulate a file using StringIO
csv_file = StringIO(csv_data)

# Define chunk size (adjust based on memory constraints)
chunk_size = 3

# Process CSV in chunks
for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size), start=1):
    # Process chunk (filter, transform, aggregate, etc.)
    print(f"Processing chunk {i} of size {len(chunk)}")
    print(chunk)
    
    # Mock S3 upload or database insertion
    print(f"Mock upload chunk {i} to S3...\n")
    # In production: s3_client.put_object(Bucket='bucket', Key=f'chunk_{i}.csv', Body=chunk.to_csv())
```

**Real-World Example: Chunk Processing with Filtering**

```python
import pandas as pd

def process_large_csv(filepath, chunk_size=10000):
    """Process large CSV file in chunks with filtering and aggregation"""
    
    aggregated_results = []
    
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        # Filter: Keep only active users
        chunk_filtered = chunk[chunk['status'] == 'active']
        
        # Transform: Calculate metrics
        chunk_filtered['revenue_per_user'] = (
            chunk_filtered['total_revenue'] / chunk_filtered['user_count']
        )
        
        # Aggregate by category
        chunk_agg = chunk_filtered.groupby('category').agg({
            'revenue_per_user': 'mean',
            'user_count': 'sum'
        }).reset_index()
        
        aggregated_results.append(chunk_agg)
    
    # Combine all chunks
    final_result = pd.concat(aggregated_results, ignore_index=True)
    
    # Final aggregation across chunks
    final_result = final_result.groupby('category').agg({
        'revenue_per_user': 'mean',
        'user_count': 'sum'
    }).reset_index()
    
    return final_result

# Usage
# result = process_large_csv('huge_file.csv', chunk_size=50000)
```

---

## Data Compression

### Compress JSON Data with Gzip

Reduce storage costs and transfer times by compressing JSON data.

```python
import gzip
import json
from io import BytesIO

# Sample event data
input_events = [
    {'user_id': 101, 'event': 'page_view', 'timestamp': '2024-01-01T10:00:00', 'page': '/home'},
    {'user_id': 102, 'event': 'click', 'timestamp': '2024-01-01T10:01:00', 'element': 'button'},
    {'user_id': 101, 'event': 'purchase', 'timestamp': '2024-01-01T10:05:00', 'amount': 100.00},
    {'user_id': 103, 'event': 'page_view', 'timestamp': '2024-01-01T10:10:00', 'page': '/products'}
]

# Convert to JSON string
json_data = json.dumps(input_events, indent=2)

# Compress JSON using gzip
buffer = BytesIO()
with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
    gz.write(json_data.encode("utf-8"))

compressed_data = buffer.getvalue()

# Display compression results
print("Uploading compressed JSON to S3 (mock)...")
print(f"Original size: {len(json_data.encode('utf-8'))} bytes")
print(f"Compressed size: {len(compressed_data)} bytes")
print(f"Compression ratio: {len(compressed_data) / len(json_data.encode('utf-8')):.2%}")
print("Upload complete (mock).")

# In production: Upload to S3
# s3_client.put_object(
#     Bucket='my-bucket',
#     Key='events/compressed_events.json.gz',
#     Body=compressed_data,
#     ContentEncoding='gzip'
# )
```

**Decompress Gzip Data**

```python
import gzip

# Decompress data
with gzip.GzipFile(fileobj=BytesIO(compressed_data), mode="rb") as gz:
    decompressed_data = gz.read().decode("utf-8")
    events = json.loads(decompressed_data)

print(events)
```

---

## JSON Processing

### Flatten Nested JSON

Transform nested JSON structures into flat tabular format using pandas.

**Example 1: Nested Objects with Arrays**

```python
import pandas as pd

input_json = {
    'user': {
        'name': 'Alice',
        'address': {
            'street': '123 Main St',
            'city': 'Paris',
            'coordinates': {
                'lat': 48.8566,
                'lon': 2.3522
            }
        }
    },
    'orders': [
        {'id': 1, 'amount': 100},
        {'id': 2, 'amount': 200}
    ]
}

# Flatten nested JSON
df1 = pd.json_normalize(
    input_json,
    record_path="orders",  # Expand orders list as rows
    meta=[
        ["user", "name"],
        ["user", "address", "street"],
        ["user", "address", "city"],
        ["user", "address", "coordinates", "lat"],
        ["user", "address", "coordinates", "lon"]
    ],
    errors="ignore"
)

print(df1)
# Output:
#    id  amount user.name user.address.street user.address.city  user.address.coordinates.lat  user.address.coordinates.lon
# 0   1     100     Alice        123 Main St             Paris                        48.8566                        2.3522
# 1   2     200     Alice        123 Main St             Paris                        48.8566                        2.3522
```

**Example 2: Multiple Nested Records**

```python
records = [
    {
        "order_id": 1,
        "customer": {"id": 101, "name": "Alice"},
        "items": [
            {"product": "A", "qty": 1},
            {"product": "B", "qty": 2}
        ]
    },
    {
        "order_id": 2,
        "customer": {"id": 102, "name": "Bob"},
        "items": [
            {"product": "C", "qty": 1}
        ]
    }
]

df2 = pd.json_normalize(
    records,
    record_path="items",
    meta=[
        'order_id',
        ['customer', 'id'],
        ['customer', 'name']
    ],
    errors="ignore"
)

print(df2)
# Output:
#   product  qty  order_id  customer.id customer.name
# 0       A    1         1          101         Alice
# 1       B    2         1          101         Alice
# 2       C    1         2          102           Bob
```

**Example 3: Deeply Nested Structures**

```python
# Complex nested API response
api_response = {
    "data": {
        "users": [
            {
                "id": 1,
                "profile": {
                    "name": "Alice",
                    "contacts": {
                        "email": "alice@example.com",
                        "phone": "123-456-7890"
                    }
                },
                "purchases": [
                    {"item": "Laptop", "price": 1200},
                    {"item": "Mouse", "price": 25}
                ]
            }
        ]
    }
}

df3 = pd.json_normalize(
    api_response['data']['users'],
    record_path='purchases',
    meta=[
        'id',
        ['profile', 'name'],
        ['profile', 'contacts', 'email']
    ]
)

print(df3)
```

---

## Statistical Analysis

### Moving Average Calculation

Calculate rolling/moving averages for time series smoothing.

```python
def moving_average(data, window_size):
    """
    Calculate moving average with specified window size.
    
    Args:
        data: List of numerical values
        window_size: Size of the moving window
        
    Returns:
        List of moving averages
    """
    result = []
    for i in range(len(data) - window_size + 1):
        window = data[i:i + window_size]
        avg = sum(window) / window_size
        result.append(avg)
    return result

# Test
input_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
window_size = 3

print(moving_average(input_data, window_size))
# Output: [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
```

**Using Pandas (More Efficient for Large Datasets)**

```python
import pandas as pd

df = pd.DataFrame({'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})

# Simple moving average
df['ma_3'] = df['value'].rolling(window=3).mean()

# Exponential moving average
df['ema_3'] = df['value'].ewm(span=3, adjust=False).mean()

print(df)
```

### Anomaly Detection with 3-Sigma Rule

Detect outliers using the statistical 3-sigma (three standard deviations) method.

```python
import numpy as np

def detect_anomalies_3sigma(data):
    """
    Detect anomalies using the 3-Sigma rule.
    
    Points beyond ±3 standard deviations from mean are considered anomalies.
    
    Args:
        data: List or array-like of numerical data
        
    Returns:
        tuple: (anomalies, lower_threshold, upper_threshold)
    """
    arr = np.array(data)
    mean = np.mean(arr)
    std = np.std(arr)
    
    lower_threshold = mean - 3 * std
    upper_threshold = mean + 3 * std
    
    # Find anomalies
    anomalies = arr[(arr < lower_threshold) | (arr > upper_threshold)]
    
    return anomalies.tolist(), lower_threshold, upper_threshold

# Test with outlier data
input_data = [1, 1, 2, 1, 1, 1, 2, 1, 1, 20, 1, 1, 2, 1, 1, 1, 2, 1]

anomalies, lower, upper = detect_anomalies_3sigma(input_data)

print(f"Mean: {np.mean(input_data):.2f}")
print(f"Std Dev: {np.std(input_data):.2f}")
print(f"Lower Threshold: {lower:.2f}")
print(f"Upper Threshold: {upper:.2f}")
print(f"Anomalies: {anomalies}")
# Output: Anomalies: [20] (the outlier)
```

**Z-Score Method (Alternative)**

```python
from scipy import stats

def detect_anomalies_zscore(data, threshold=3):
    """Detect anomalies using Z-score method"""
    z_scores = np.abs(stats.zscore(data))
    anomaly_indices = np.where(z_scores > threshold)[0]
    anomalies = [data[i] for i in anomaly_indices]
    return anomalies, anomaly_indices

# Test
anomalies, indices = detect_anomalies_zscore(input_data)
print(f"Anomalies at indices {indices}: {anomalies}")
```

---

## Algorithm Patterns

### Merge Two Sorted Lists

Efficient O(n + m) algorithm to merge two sorted lists.

```python
def merge_sorted_lists(list1, list2):
    """
    Merge two sorted lists into one sorted list.
    
    Time Complexity: O(n + m)
    Space Complexity: O(n + m)
    """
    merged = []
    i, j = 0, 0
    n, m = len(list1), len(list2)
    
    # Merge while both lists have elements
    while i < n and j < m:
        if list1[i] <= list2[j]:
            merged.append(list1[i])
            i += 1
        else:
            merged.append(list2[j])
            j += 1
    
    # Append remaining elements from list1
    while i < n:
        merged.append(list1[i])
        i += 1
    
    # Append remaining elements from list2
    while j < m:
        merged.append(list2[j])
        j += 1
    
    return merged

# Test
list1 = [1, 3, 5, 7, 9, 11, 13, 15]
list2 = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

result = merge_sorted_lists(list1, list2)
print(result)
# Output: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20]
```

**Alternative: Using Python Built-ins**

```python
import heapq

# Simple concatenation and sort
merged = sorted(list1 + list2)

# Using heapq.merge (more efficient for large lists)
merged = list(heapq.merge(list1, list2))
```

### Array Intersection

Find common elements between two arrays in O(n) time.

```python
def array_intersection(arr1, arr2):
    """
    Find intersection of two arrays efficiently.
    
    Time Complexity: O(n + m)
    Space Complexity: O(min(n, m))
    """
    # Convert one array to set for O(1) lookups
    set1 = set(arr1)
    
    # Check each element of arr2
    intersection = [x for x in arr2 if x in set1]
    
    # Remove duplicates
    return list(set(intersection))

# Test
arr1 = [1, 2, 3, 4]
arr2 = [3, 4, 5, 6]

result = array_intersection(arr1, arr2)
print(result)
# Output: [3, 4]
```

**Maintaining Order and Duplicates**

```python
def array_intersection_ordered(arr1, arr2):
    """Keep order from arr2 and preserve duplicates"""
    set1 = set(arr1)
    return [x for x in arr2 if x in set1]

# Using set intersection
def array_intersection_set(arr1, arr2):
    """Pure set intersection (no duplicates, no order)"""
    return list(set(arr1) & set(arr2))
```

### User Sessionization

Group user events into sessions based on time threshold (e.g., 30-minute inactivity).

```python
from datetime import datetime, timedelta

# Input events
events = [
    {'user_id': 101, 'timestamp': '2024-01-01 10:00:00', 'action': 'login'},
    {'user_id': 101, 'timestamp': '2024-01-01 10:10:00', 'action': 'view_page'},
    {'user_id': 101, 'timestamp': '2024-01-01 10:40:00', 'action': 'click'},  # New session
    {'user_id': 102, 'timestamp': '2024-01-01 11:00:00', 'action': 'login'},
    {'user_id': 101, 'timestamp': '2024-01-01 11:05:00', 'action': 'logout'},
    {'user_id': 101, 'timestamp': '2024-01-01 11:50:00', 'action': 'login'}   # New session
]

# Convert timestamps to datetime objects
for event in events:
    event['timestamp'] = datetime.strptime(event['timestamp'], '%Y-%m-%d %H:%M:%S')

# Sort by user_id and timestamp
events.sort(key=lambda x: (x['user_id'], x['timestamp']))

# Sessionization threshold (30 minutes)
threshold = timedelta(minutes=30)

# Initialize session tracking
user_last_time = {}      # Last event timestamp per user
user_session_id = {}     # Current session ID per user

for event in events:
    user = event['user_id']
    ts = event['timestamp']
    
    if user not in user_last_time:
        # First event for user → start new session
        user_session_id[user] = 1
    else:
        # Check time gap with last event
        if ts - user_last_time[user] > threshold:
            user_session_id[user] += 1  # New session
    
    # Assign session ID
    event['session_id'] = user_session_id[user]
    user_last_time[user] = ts

# Display results
for e in events:
    print(f"User {e['user_id']}, Session {e['session_id']}: "
          f"{e['action']} at {e['timestamp']}")

# Output:
# User 101, Session 1: login at 2024-01-01 10:00:00
# User 101, Session 1: view_page at 2024-01-01 10:10:00
# User 101, Session 2: click at 2024-01-01 10:40:00
# User 101, Session 2: logout at 2024-01-01 11:05:00
# User 101, Session 3: login at 2024-01-01 11:50:00
# User 102, Session 1: login at 2024-01-01 11:00:00
```

**Using Pandas for Sessionization**

```python
import pandas as pd
from datetime import timedelta

df = pd.DataFrame(events)
df = df.sort_values(['user_id', 'timestamp'])

# Calculate time difference from previous event per user
df['time_diff'] = df.groupby('user_id')['timestamp'].diff()

# Mark new sessions where time_diff > threshold
df['new_session'] = (df['time_diff'] > timedelta(minutes=30)) | (df['time_diff'].isna())

# Cumulative sum to get session IDs
df['session_id'] = df.groupby('user_id')['new_session'].cumsum()

print(df[['user_id', 'timestamp', 'action', 'session_id']])
```

---

## ETL Pipelines

### Mini ETL Pipeline

Complete Extract-Transform-Load example with data quality checks.

```python
import pandas as pd
from io import StringIO

# ===== EXTRACT =====
csv_content = """user_id,name,age,city,order_amount
101,Alice,30,Paris,100.50
102,Bob,25,London,200.00
103,Charlie,35,Berlin,150.75
104,Diana,28,Rome,300.25
105,,32,Madrid,180.00
106,Frank,29,,220.50
107,Grace,31,Brussels,275.00"""

# Read CSV
df = pd.read_csv(StringIO(csv_content))

print("=" * 50)
print("EXTRACT: Original Data")
print("=" * 50)
print(df)
print(f"\nShape: {df.shape}")
print(f"Null values:\n{df.isnull().sum()}")

# ===== TRANSFORM =====
print("\n" + "=" * 50)
print("TRANSFORM: Data Cleaning")
print("=" * 50)

# 1. Fill missing names
df['name'] = df['name'].fillna('Unknown')

# 2. Fill missing cities
df['city'] = df['city'].fillna('Unknown')

# 3. Ensure order_amount is numeric
df['order_amount'] = pd.to_numeric(df['order_amount'], errors='coerce')

# 4. Add derived columns
df['order_category'] = pd.cut(
    df['order_amount'],
    bins=[0, 150, 250, float('inf')],
    labels=['Low', 'Medium', 'High']
)

# 5. Add validation flags
df['is_valid'] = (
    (df['name'] != 'Unknown') &
    (df['city'] != 'Unknown') &
    (df['order_amount'].notna())
)

# 6. Filter out invalid rows (optional)
df_clean = df[df['is_valid']].copy()

print("\nTransformed Data:")
print(df_clean)
print(f"\nRows removed: {len(df) - len(df_clean)}")

# ===== LOAD =====
print("\n" + "=" * 50)
print("LOAD: Save to Output")
print("=" * 50)

# Save to CSV
output_file = 'cleaned_users.csv'
df_clean.to_csv(output_file, index=False)
print(f"✓ Data saved to '{output_file}'")

# Mock database load
print("✓ Mock database insert complete")

# Generate data quality report
print("\n" + "=" * 50)
print("DATA QUALITY REPORT")
print("=" * 50)
print(f"Total records processed: {len(df)}")
print(f"Valid records: {len(df_clean)}")
print(f"Invalid records: {len(df) - len(df_clean)}")
print(f"Data quality score: {len(df_clean) / len(df) * 100:.1f}%")

# Summary statistics
print("\nSummary Statistics:")
print(df_clean[['age', 'order_amount']].describe())
```

**Advanced ETL with Error Handling**

```python
import logging
from typing import Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics = {
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'errors': []
        }
    
    def extract(self, source: str) -> pd.DataFrame:
        """Extract data from source"""
        try:
            logger.info(f"Extracting data from {source}")
            df = pd.read_csv(source)
            self.metrics['extracted'] = len(df)
            logger.info(f"Extracted {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            self.metrics['errors'].append(str(e))
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformations"""
        try:
            logger.info("Starting transformations")
            
            # Apply transformation rules
            df_transformed = df.copy()
            
            # Add transformations here
            df_transformed = self._clean_data(df_transformed)
            df_transformed = self._enrich_data(df_transformed)
            df_transformed = self._validate_data(df_transformed)
            
            self.metrics['transformed'] = len(df_transformed)
            logger.info(f"Transformed {len(df_transformed)} rows")
            return df_transformed
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            self.metrics['errors'].append(str(e))
            raise
    
    def load(self, df: pd.DataFrame, destination: str):
        """Load data to destination"""
        try:
            logger.info(f"Loading data to {destination}")
            df.to_csv(destination, index=False)
            self.metrics['loaded'] = len(df)
            logger.info(f"Loaded {len(df)} rows successfully")
        except Exception as e:
            logger.error(f"Load failed: {e}")
            self.metrics['errors'].append(str(e))
            raise
    
    def _clean_data(self, df):
        """Data cleaning logic"""
        df = df.fillna({'name': 'Unknown', 'city': 'Unknown'})
        return df
    
    def _enrich_data(self, df):
        """Data enrichment logic"""
        df['processed_at'] = pd.Timestamp.now()
        return df
    
    def _validate_data(self, df):
        """Data validation logic"""
        return df[df['order_amount'].notna()]
    
    def run(self, source: str, destination: str):
        """Run complete ETL pipeline"""
        logger.info("=" * 50)
        logger.info("Starting ETL Pipeline")
        logger.info("=" * 50)
        
        try:
            # Extract
            df = self.extract(source)
            
            # Transform
            df_transformed = self.transform(df)
            
            # Load
            self.load(df_transformed, destination)
            
            logger.info("=" * 50)
            logger.info("ETL Pipeline Completed Successfully")
            logger.info(f"Metrics: {self.metrics}")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise

# Usage
# config = {'chunk_size': 10000, 'validate': True}
# pipeline = ETLPipeline(config)
# pipeline.run('input.csv', 'output.csv')
```

---

## Best Practices

### Performance Tips

1. **Use generators for large datasets** to save memory
2. **Leverage pandas vectorization** instead of loops
3. **Process data in chunks** for files larger than available RAM
4. **Use appropriate data types** (e.g., `category` for repeated strings)
5. **Profile your code** with `cProfile` or `line_profiler`

### Code Quality

1. **Add type hints** for better code documentation
2. **Write unit tests** for critical functions
3. **Use logging** instead of print statements
4. **Handle exceptions** gracefully
5. **Document assumptions** and edge cases

### Data Quality

1. **Validate input data** before processing
2. **Track metrics** (rows processed, errors, quality score)
3. **Implement idempotency** for ETL pipelines
4. **Add data quality checks** at each stage
5. **Log anomalies** for investigation

---

*This guide covers common data engineering patterns in Python. Adapt these patterns to your specific use cases and requirements.*
