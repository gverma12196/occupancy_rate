import pandas as pd

# Load the CSV files
profile = pd.read_csv('mykonos_property_details.csv')
per = pd.read_csv('mykonos_monthly_performance.csv')

# Check column names (equivalent to colnames in R)
print(profile.columns)

# Merge datasets
dt = pd.merge(
    profile[['property_id', 'adm_1_id', 'adm_2_id', 'adm_3_id', 'bathrooms', 'bedrooms']],
    per[['property_id', 'year', 'month', 'occupancy']],
    on='property_id',
    how='outer'
)

# Create dt_non for adding entries with zero occupancy
dt_non = dt.copy()

# Create a list to store the new dataframes for each month
new_dfs = []

# Create dataframes for each month with occupancy=0
for month in range(1, 13):
    new_df = dt_non[['property_id', 'adm_1_id', 'adm_2_id', 'adm_3_id', 'bathrooms', 'bedrooms']].copy()
    new_df['occupancy'] = 0
    new_df['year'] = 2024
    new_df['month'] = month
    new_dfs.append(new_df)

# Combine all the new dataframes
dt_non = pd.concat(new_dfs)

# Combine dt (where occupancy is not NA) with dt_non
dt = pd.concat([dt.dropna(subset=['occupancy']), dt_non])

# Group by year, month, property_id and sum occupancy
dt = dt.groupby(['year', 'month', 'property_id']).agg({'occupancy': 'sum'}).reset_index()

# Count rows per property_id, year, month (equivalent to dt[,.N,.(property_id,year,month)])
counts = dt.groupby(['property_id', 'year', 'month']).size().reset_index(name='N')
counts = counts.sort_values('N', ascending=False)
print(counts)

# Calculate mean occupancy by region (year, month)
dt_with_region = dt.copy()
dt_with_region['occupancy_region'] = dt_with_region.groupby(['year', 'month'])['occupancy'].transform('mean')
dt_with_region.to_excel('mykonos_rate_output.xlsx')