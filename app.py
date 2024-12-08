import argparse
import logging
from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
from pydantic import BaseModel
from typing import List, Dict, Any

logging.basicConfig(level='INFO', format='%(asctime)s [%(levelname)s]: %(message)s')
logger = logging.getLogger(__name__)

class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def create_user(self, user_data):
        query = """
        MERGE (u:User {id: $id})
        SET u.name = $name, u.screen_name = $screen_name, u.sex = $sex, u.city = $city
        """
        self.run_query(query, user_data)

    def create_group(self, group_data):
        query = """
        MERGE (g:Group {id: $id})
        SET g.name = $name, g.screen_name = $screen_name
        """
        self.run_query(query, group_data)

    def rel_follow(self, user_from, user_to):
        query = """
        MATCH (u1:User {id: $from})
        MATCH (u2:User {id: $to})
        MERGE (u1)-[:FOLLOWS]->(u2)
        """
        self.run_query(query, {'from': user_from, 'to': user_to})

    def rel_sub(self, user, group):
        query = """
        MATCH (u:User {id: $user})
        MATCH  (g:Group {id: $group})
        MERGE (u)-[:SUBSCRIBED]->(g)
        """
        self.run_query(query, {'user': user, 'group': group})

    def query(self, query_type):
        queries = {
            'users_count': "MATCH (u:User) RETURN COUNT(u) AS count",
            'groups_count': "MATCH (g:Group) RETURN COUNT(g) AS count",
            'top_users': """
                MATCH (u:User)<-[:FOLLOWS]-()
                RETURN u.id, u.name, COUNT(*) AS followers_count
                ORDER BY followers_count DESC LIMIT 5
            """,
            'top_groups': """
                MATCH (g:Group)<-[:SUBSCRIBED]-()
                RETURN g.id, g.name, COUNT(*) AS subscribers_count
                ORDER BY subscribers_count DESC LIMIT 5
            """,
            'mutual_followers': """
                MATCH (u1:User)-[:FOLLOWS]->(u2:User)-[:FOLLOWS]->(u1) 
                RETURN u1.id, u2.id
            """
        }

        try:
            result = self.run_query(queries[query_type])
            return result
        except KeyError:
            logger.error("Такого запроса нет")
            return []


class UserInfo(BaseModel):
    user_id: str
    depth: int = 2


app = FastAPI()


neo4j_handler = Neo4jHandler(uri="neo4j://localhost:7687", user="neo4j", password="11111111")


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    logger.info(f"Fetching user with ID: {user_id}")
    query = "MATCH (u:User {id: toInteger($user_id)}) RETURN u.id, u.name, u.screen_name, u.sex, u.city"
    result = neo4j_handler.run_query(query, {'user_id': user_id})

    if not result:
        logger.error(f"User with ID {user_id} not found")
        raise HTTPException(status_code=404, detail="User not found")
    
    user = result[0]
    return {
        "id": user['u.id'],
        "name": user['u.name'],
        "screen_name": user['u.screen_name'],
        "sex": user['u.sex'],
        "city": user['u.city']
    }


@app.get("/top-users")
async def get_top_users():
    query = """
    MATCH (u:User)<-[:FOLLOWS]-()
    RETURN u.id, u.name, COUNT(*) AS followers_count
    ORDER BY followers_count DESC LIMIT 5
    """
    result = neo4j_handler.run_query(query)

    return [{"id": record["u.id"], "name": record["u.name"], "followers_count": record["followers_count"]} for record in result]


@app.get("/top-groups")
async def get_top_groups():
    query = """
    MATCH (g:Group)<-[:SUBSCRIBED]-()
    RETURN g.id, g.name, COUNT(*) AS subscribers_count
    ORDER BY subscribers_count DESC LIMIT 5
    """
    result = neo4j_handler.run_query(query)

    return [{"id": record["g.id"], "name": record["g.name"], "subscribers_count": record["subscribers_count"]} for record in result]

@app.get("/users-count")
async def get_users_count():
    query = "MATCH (u:User) RETURN COUNT(u) AS count"
    result = neo4j_handler.run_query(query)

    return {"users_count": result[0]["count"]}

@app.get("/groups-count")
async def get_groups_count():
    query = "MATCH (g:Group) RETURN COUNT(g) AS count"
    result = neo4j_handler.run_query(query)

    return {"groups_count": result[0]["count"]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
