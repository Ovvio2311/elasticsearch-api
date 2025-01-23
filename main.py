import requests
import csv
import os
from datetime import datetime, timedelta

# Function to get the count of documents matching the criteria from Kibana
def get_count_from_kibana():
    url = "http://localhost:9200/your_index/_search"
    auth = ('elastic', 'elastic')
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": "now-1d/d",
                                "lte": "now/d",
                                "time_zone": "+08:00"
                            }
                        }
                    },
                    {
                        "match": {
                            "your_field1": "your_value1"
                        }
                    },
                    {
                        "match": {
                            "your_field2": "your_value2"
                        }
                    }
                ]
            }
        },
        "size": 0,
        "track_total_hits": True
    }

    response = requests.post(url, auth=auth, headers=headers, json=query)
    response_data = response.json()
    count = response_data['hits']['total']['value']
    return count

# Function to write the count to a CSV file
def write_count_to_csv(date, count):
    month = date.strftime("%Y-%m")
    csv_filename = f"{month}_counts.csv"
    
    # Check if the CSV file exists
    file_exists = os.path.isfile(csv_filename)
    
    # Open the CSV file in append mode
    with open(csv_filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header if the file does not exist
        if not file_exists:
            writer.writerow(['Date', 'Count'])
        
        # Write the date and count
        writer.writerow([date.strftime("%Y-%m-%d"), count])

# Function to sum up the counts for the month and write to a summary CSV file
def summarize_monthly_counts():
    today = datetime.now()
    month = today.strftime("%Y-%m")
    csv_filename = f"{month}_counts.csv"
    
    if os.path.isfile(csv_filename):
        total_count = 0
        
        # Read the counts from the CSV file and sum them up
        with open(csv_filename, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                total_count += int(row['Count'])
        
        # Write the summary to a new CSV file
        summary_filename = f"{month}_summary.csv"
        with open(summary_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Month', 'Total Count'])
            writer.writerow([month, total_count])

# Main function to run the script daily
def main():
    # Get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    
    # Get the count from Kibana
    count = get_count_from_kibana()
    
    # Write the count to the CSV file
    write_count_to_csv(yesterday, count)
    
    # Summarize the monthly counts at the end of the month
    if yesterday.day == 1:
        summarize_monthly_counts()

# Run the main function
if __name__ == "__main__":
    main()
