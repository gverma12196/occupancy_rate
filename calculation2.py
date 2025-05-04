import os
import pandas as pd
import numpy as np
from datetime import datetime

# Set working directory
os.chdir("C://Users//gverm//Downloads//Job Hunt//Investimate//data-dump//data-dump/")

# Load data
profile = pd.read_csv('paros_property_details.csv')
per = pd.read_csv('paros_monthly_performance.csv')

# Check data distribution
monthly_counts = per.groupby(['month', 'year', 'property_id']).size().reset_index(name='count')
year_month_counts = monthly_counts.groupby(['year', 'month']).size().reset_index(name='count')
print(year_month_counts.sort_values('month', ascending=False))

# Merge property details with performance data
profile_subset = profile[['property_id', 'adm_1_id', 'adm_2_id', 'adm_3_id', 'bathrooms', 'bedrooms']]
per_subset = per[['property_id', 'year', 'month', 'occupancy', 'adr', 'revenue', 'revpar', 'fetched_revenue']]
dt = pd.merge(profile_subset, per_subset, on='property_id', how='outer')
dt_non = dt.copy()

# Get the unique property IDs and create a property-to-bedroom mapping
all_properties = dt_non['property_id'].unique()
property_bedroom_map = dt_non[['property_id', 'bedrooms']].drop_duplicates().set_index('property_id')

# Create a list to store all the monthly dataframes
monthly_dfs = []

# For each month in 2024, create entries for all properties
for i in range(1, 13):
    # Create a dataframe for properties
    month_df = pd.DataFrame({'property_id': all_properties})
    
    # Add bedroom information using the mapping
    month_df = month_df.join(property_bedroom_map, on='property_id')
    
    # Add the other columns with zeros
    month_df['adr'] = 0
    month_df['revenue'] = 0
    month_df['revpar'] = 0
    month_df['fetched_revenue'] = 0
    month_df['occupancy'] = 0
    month_df['year'] = 2024
    month_df['month'] = i
    
    # Add to our list
    monthly_dfs.append(month_df)
    print(f"Processed month {i}")

# Start with the original data
dt_subset = dt_non[['adr', 'revenue', 'revpar', 'fetched_revenue', 'occupancy', 
                   'year', 'month', 'property_id', 'bedrooms']].drop_duplicates()

# Combine with our monthly data
for df in monthly_dfs:
    dt_subset = pd.concat([dt_subset, df])

# Filter out rows with missing month info
dt_non = dt_subset.dropna(subset=['month'])

# Make sure bedrooms column is not null for later calculations
dt_non = dt_non.dropna(subset=['bedrooms'])

# Count properties per ID
pro = dt_non.groupby('property_id').size().reset_index(name='count')

# Create a copy for yearly calculations
dt_non_year = dt_non.copy()
dt_non_year = dt_non_year.drop(columns=['month'])

# Aggregate by year, month, property_id, and bedrooms
dt_non = dt_non.groupby(['year', 'month', 'property_id', 'bedrooms']).agg({
    'occupancy': 'sum',
    'adr': 'sum',
    'revenue': 'sum',
    'revpar': 'sum',
    'fetched_revenue': 'sum'
}).reset_index()

# Aggregate by year, property_id, and bedrooms for yearly metrics
dt_non_year = dt_non_year.groupby(['year', 'property_id', 'bedrooms']).agg({
    'occupancy': lambda x: sum(x) / 12,
    'adr': lambda x: sum(x) / 12,
    'revenue': lambda x: sum(x) / 12,
    'revpar': lambda x: sum(x) / 12,
    'fetched_revenue': lambda x: sum(x) / 12
}).reset_index()

# Rename columns for yearly metrics
dt_non_year = dt_non_year.rename(columns={
    'occupancy': 'occupancy_year',
    'adr': 'adr_year',
    'revenue': 'revenue_year',
    'revpar': 'revpar_year',
    'fetched_revenue': 'fetched_revenue_year'
})

# Calculate yearly total properties
dt_paros_total_properties_year = pd.merge(
    dt_non_year.groupby(['year', 'bedrooms']).size().reset_index(name='total_properties_adj_year'),
    dt_non_year[dt_non_year['occupancy_year'] > 0].groupby(['year', 'bedrooms']).size().reset_index(name='total_properties_raw_year'),
    on=['year', 'bedrooms']
)

# Calculate yearly occupancy metrics
dt_paros_occupancy_year = pd.merge(
    dt_non_year.groupby(['year', 'bedrooms'])['occupancy_year'].mean().reset_index().rename(columns={'occupancy_year': 'occupancy_adj_year'}),
    dt_non_year[dt_non_year['occupancy_year'] > 0].groupby(['year', 'bedrooms'])['occupancy_year'].mean().reset_index().rename(columns={'occupancy_year': 'occupancy_raw_year'}),
    on=['year', 'bedrooms']
)

# Calculate yearly ADR metrics
dt_paros_adr_year = pd.merge(
    dt_non_year.groupby(['year', 'bedrooms'])['adr_year'].mean().reset_index().rename(columns={'adr_year': 'adr_adj_year'}),
    dt_non_year[dt_non_year['occupancy_year'] > 0].groupby(['year', 'bedrooms'])['adr_year'].mean().reset_index().rename(columns={'adr_year': 'adr_raw_year'}),
    on=['year', 'bedrooms']
)

# Calculate yearly revenue metrics
dt_paros_revenue_year = pd.merge(
    dt_non_year.groupby(['year', 'bedrooms'])['revenue_year'].mean().reset_index().rename(columns={'revenue_year': 'revenue_adj_year'}),
    dt_non_year[dt_non_year['occupancy_year'] > 0].groupby(['year', 'bedrooms'])['revenue_year'].mean().reset_index().rename(columns={'revenue_year': 'revenue_raw_year'}),
    on=['year', 'bedrooms']
)

# Calculate monthly total properties
dt_paros_total_properties = pd.merge(
    dt_non.groupby(['year', 'month', 'bedrooms']).size().reset_index(name='total_properties_adj'),
    dt_non[dt_non['occupancy'] > 0].groupby(['year', 'month', 'bedrooms']).size().reset_index(name='total_properties_raw'),
    on=['year', 'month', 'bedrooms']
)

# Calculate monthly occupancy metrics
dt_paros_occupancy = pd.merge(
    dt_non.groupby(['year', 'month', 'bedrooms'])['occupancy'].mean().reset_index().rename(columns={'occupancy': 'occupancy_adj'}),
    dt_non[dt_non['occupancy'] > 0].groupby(['year', 'month', 'bedrooms'])['occupancy'].mean().reset_index().rename(columns={'occupancy': 'occupancy_raw'}),
    on=['year', 'month', 'bedrooms']
)

# Calculate monthly ADR metrics
dt_paros_adr = pd.merge(
    dt_non.groupby(['year', 'month', 'bedrooms'])['adr'].mean().reset_index().rename(columns={'adr': 'adr_adj'}),
    dt_non[dt_non['occupancy'] > 0].groupby(['year', 'month', 'bedrooms'])['adr'].mean().reset_index().rename(columns={'adr': 'adr_raw'}),
    on=['year', 'month', 'bedrooms']
)

# Calculate monthly revenue metrics
dt_paros_revenue = pd.merge(
    dt_non.groupby(['year', 'month', 'bedrooms'])['revenue'].mean().reset_index().rename(columns={'revenue': 'revenue_adj'}),
    dt_non[dt_non['occupancy'] > 0].groupby(['year', 'month', 'bedrooms'])['revenue'].mean().reset_index().rename(columns={'revenue': 'revenue_raw'}),
    on=['year', 'month', 'bedrooms']
)

# Merge all monthly metrics
dt_paros = pd.merge(dt_paros_revenue, dt_paros_total_properties, on=['year', 'month', 'bedrooms'])
dt_paros = pd.merge(dt_paros, dt_paros_adr, on=['year', 'month', 'bedrooms'])
dt_paros = pd.merge(dt_paros, dt_paros_occupancy, on=['year', 'month', 'bedrooms'])

# Merge all yearly metrics
dt_paros_year = pd.merge(dt_paros_revenue_year, dt_paros_total_properties_year, on=['year', 'bedrooms'])
dt_paros_year = pd.merge(dt_paros_year, dt_paros_adr_year, on=['year', 'bedrooms'])
dt_paros_year = pd.merge(dt_paros_year, dt_paros_occupancy_year, on=['year', 'bedrooms'])

# Reshape monthly data using pandas pivot
value_vars = ["revenue_adj", "revenue_raw", "total_properties_adj", "total_properties_raw", 
              "adr_adj", "adr_raw", "occupancy_adj", "occupancy_raw"]
dt_paros2 = pd.DataFrame()

for value_var in value_vars:
    # Use pivot to reshape the data
    pivot_df = dt_paros.pivot(index=['year', 'bedrooms'], columns='month', values=value_var)
    
    # Rename columns to include the value variable name
    pivot_df.columns = [f"{value_var}_{col}" for col in pivot_df.columns]
    
    # If this is the first variable, create the dataframe, otherwise join
    if dt_paros2.empty:
        dt_paros2 = pivot_df.reset_index()
    else:
        pivot_df = pivot_df.reset_index()
        dt_paros2 = pd.merge(dt_paros2, pivot_df, on=['year', 'bedrooms'])

# Merge with yearly data
dt_paros_final = pd.merge(dt_paros2, dt_paros_year, on=['year', 'bedrooms'])

# Export to CSV
dt_paros_final.to_csv('paros_all_in_one.csv', index=False)