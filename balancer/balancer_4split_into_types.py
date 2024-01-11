import pandas as pd

df = pd.read_csv('balancer-type.csv')

keywords = ['Stable', 'Weighted', 'Linear']

mask1 = df['first_key'].str.contains('Stable', case=False, regex=False)
mask2 = df['first_key'].str.contains('Weighted', case=False, regex=False)
mask3 = df['first_key'].str.contains('Linear', case=False, regex=False)

# Use the boolean masks to filter the original DataFrame into three smaller DataFrames
df1 = df[mask1]
df2 = df[mask2]
df3 = df[mask3]

# Reset index for each smaller DataFrame (optional)
df1 = df1.reset_index(drop=True)
df2 = df2.reset_index(drop=True)
df3 = df3.reset_index(drop=True)

# Print or use the three DataFrames: df1, df2, df3
print(df1)
print(df2)
print(df3)

df1.to_csv("balancer-stable.csv", index=False)
df2.to_csv("balancer-weighted.csv", index=False)
df3.to_csv("balancer-linear.csv", index=False)