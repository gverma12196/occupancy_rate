from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from schema import schema

app = FastAPI(title="Mykonos Property API")

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql") 