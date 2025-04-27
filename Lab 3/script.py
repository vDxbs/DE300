import pandas as pd

df = pd.DataFrame({
    'Name': ['Alice', 'Bob'],
    'Age': [25, 30]
})

df.to_csv('csv-output/ec2_output.csv', index=False)
print("CSV file created!")