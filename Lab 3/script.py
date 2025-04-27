import pandas as pd

df = pd.DataFrame({
    'Name': ['Alice', 'Bob'],
    'Age': [25, 30]
})

os.makedirs('csv-output', exist_ok=True)
df.to_csv('csv-output/ec2_output.csv', index=False)
print("CSV file created!")