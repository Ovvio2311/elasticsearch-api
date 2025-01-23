import requests
import csv
import os
from datetime import datetime, timedelta

# Function to get the count of documents matching the criteria from Kibana
def get_count_from_kibana(yesterday):
    url = "http://localhost:9200/your_index/_search"
    auth = ('your_username', 'your_password')
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": f"now-{yesterday}d/d",
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
    }

    response = requests.post(url, auth=auth, headers=headers, json=query, verify=False)
    response_data = response.json()
    count = response_data['count']
    return count

# Function to write the count to a CSV file
def write_count_to_csv(date, count):
    month = date.strftime("%Y-%m")
    csv_filename = f"{month}_counts.csv"
    
    # Read existing data from the CSV file
    data = []
    if os.path.isfile(csv_filename):
        with open(csv_filename, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data.append(row)
    
    # Check if the date already exists in the data
    date_str = date.strftime("%Y-%m-%d")
    date_exists = False
    for row in data:
        if row['Date'] == date_str:
            row['Count'] = count
            date_exists = True
            break
    
    # If the date does not exist, append the new data
    if not date_exists:
        data.append({'Date': date_str, 'Count': count})
    
    # Write the updated data back to the CSV file
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Date', 'Count'])
        writer.writeheader()
        writer.writerows(data)

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
    day = 2
    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=day)).day
    
    # Get the count from Kibana
    count = get_count_from_kibana(day)
    
    # Write the count to the CSV file
    write_count_to_csv(datetime.now() - timedelta(days=day), count)
    
    # Summarize the monthly counts at the end of the month
    if (datetime.now() - timedelta(days=day)).day == 1:
        summarize_monthly_counts()

# Run the main function
if __name__ == "__main__":
    main()
