import pandas as pd

df = pd.DataFrame({
    'Name': ['Alice', 'Bob'],
    'Age': [25, 30]
})

df.to_csv('/app/output/ec2_output.csv', index=False)
print("CSV file created!")
