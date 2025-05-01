import strawberry
from typing import List, Optional
import pandas as pd
from datetime import datetime

@strawberry.type
class Property:
    property_id: str
    adm_1_id: str
    adm_2_id: str
    adm_3_id: str
    bathrooms: int
    bedrooms: int

@strawberry.type
class MonthlyPerformance:
    property_id: str
    year: int
    month: int
    occupancy: float

@strawberry.type
class PropertyPerformance:
    property: Property
    monthly_performances: List[MonthlyPerformance]

@strawberry.type
class RegionPerformance:
    year: int
    month: int
    average_occupancy: float

@strawberry.type
class Query:
    @strawberry.field
    async def properties(self) -> List[Property]:
        profile = pd.read_csv('mykonos_property_details.csv')
        return [
            Property(
                property_id=str(row['property_id']),
                adm_1_id=str(row['adm_1_id']),
                adm_2_id=str(row['adm_2_id']),
                adm_3_id=str(row['adm_3_id']),
                bathrooms=int(row['bathrooms']),
                bedrooms=int(row['bedrooms'])
            )
            for _, row in profile.iterrows()
        ]

    @strawberry.field
    async def property_performance(
        self,
        property_id: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> List[PropertyPerformance]:
        profile = pd.read_csv('mykonos_property_details.csv')
        per = pd.read_csv('mykonos_monthly_performance.csv')
        
        # Merge datasets
        dt = pd.merge(
            profile[['property_id', 'adm_1_id', 'adm_2_id', 'adm_3_id', 'bathrooms', 'bedrooms']],
            per[['property_id', 'year', 'month', 'occupancy']],
            on='property_id',
            how='outer'
        )

        # Apply filters if provided
        if property_id:
            dt = dt[dt['property_id'] == property_id]
        if year:
            dt = dt[dt['year'] == year]
        if month:
            dt = dt[dt['month'] == month]

        # Group by property and create PropertyPerformance objects
        result = []
        for prop_id, group in dt.groupby('property_id'):
            property_data = group.iloc[0]
            property_obj = Property(
                property_id=str(property_data['property_id']),
                adm_1_id=str(property_data['adm_1_id']),
                adm_2_id=str(property_data['adm_2_id']),
                adm_3_id=str(property_data['adm_3_id']),
                bathrooms=int(property_data['bathrooms']),
                bedrooms=int(property_data['bedrooms'])
            )
            
            performances = [
                MonthlyPerformance(
                    property_id=str(row['property_id']),
                    year=int(row['year']),
                    month=int(row['month']),
                    occupancy=float(row['occupancy'])
                )
                for _, row in group.iterrows()
            ]
            
            result.append(PropertyPerformance(
                property=property_obj,
                monthly_performances=performances
            ))
        
        return result

    @strawberry.field
    async def region_performance(
        self,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> List[RegionPerformance]:
        per = pd.read_csv('mykonos_monthly_performance.csv')
        
        # Apply filters if provided
        if year:
            per = per[per['year'] == year]
        if month:
            per = per[per['month'] == month]
        
        # Calculate average occupancy by year and month
        result = []
        for (y, m), group in per.groupby(['year', 'month']):
            result.append(RegionPerformance(
                year=int(y),
                month=int(m),
                average_occupancy=float(group['occupancy'].mean())
            ))
        
        return result

schema = strawberry.Schema(query=Query) 