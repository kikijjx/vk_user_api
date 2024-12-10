import argparse
import logging
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
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
            try:
                result = session.run(query, parameters)
                return [record for record in result]
            except:
                logger.error(f"Ошибка при выполнении запроса")
                raise HTTPException(status_code=500, detail="Ошибка выполнения запроса")

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
    
    def create_relationship(self, from_node_id, from_node_label, to_node_id, to_node_label, relationship_type):
        query = """
        MATCH (a:{from_node_label} {{id: $from_node_id}}),
              (b:{to_node_label} {{id: $to_node_id}})
        MERGE (a)-[r:{relationship_type}]->(b)
        SET r += $attributes
        """
        self.run_query(query.format(
            from_node_label=from_node_label,
            to_node_label=to_node_label,
            relationship_type=relationship_type
        ), {
            "from_node_id": from_node_id,
            "to_node_id": to_node_id,
            "attributes": {}
        })

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

class NodeData(BaseModel):
    label: str
    id: int
    attributes: Dict[str, Any]

class RelationshipData(BaseModel):
    from_node: str
    to_node: str
    relationship_type: str
    attributes: Dict[str, Any]

app = FastAPI()

security = HTTPBearer()
neo4j_handler = Neo4jHandler(uri="neo4j://localhost:7687", user="neo4j", password="11111111")
SECRET_TOKEN: str = "tokenchik"

def token_is_valid(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != SECRET_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.credentials
    
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

@app.get("/nodes")
async def get_all_nodes():
    query = "MATCH (n) RETURN n.id, labels(n) AS label"
    result = neo4j_handler.run_query(query)

    nodes = [{"id": record["n.id"], "label": record["label"][0]} for record in result]
    return nodes

@app.get("/node/{label}/{node_id}")
async def get_node_with_relations(label: str, node_id: int):
    """
    Получить узел и его связи по label и id.
    """
    logger.info(f"Fetching node with label: {label} and ID: {node_id}")
    
    query = f"""
    MATCH (n:{label} {{id: $node_id}})
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN n, COLLECT(r) AS relationships, COLLECT(m) AS related_nodes
    """
    result = neo4j_handler.run_query(query, {"node_id": node_id})

    if not result:
        logger.error(f"No results found for node with label: {label} and ID: {node_id}")
        raise HTTPException(status_code=404, detail="Node or relations not found")

    node_data = None
    relations = []

    for record in result:
        node = record['n']
        relationships = record['relationships']
        related_nodes = record['related_nodes']

        if node_data is None:
            node_data = {
                "label": label,
                "attributes": dict(node.items())
            }

        for relationship, related_node in zip(relationships, related_nodes):
            relations.append({
                "relationship": {
                    "type": relationship.type if relationship else None,
                    "attributes": dict(relationship.items()) if relationship else {}
                },
                "related_node": {
                    "id": related_node['id'],
                    "label": list(related_node.labels)[0] if related_node.labels else 'No Label',
                    "attributes": dict(related_node.items()) if related_node else {}
                }
            })

    return {
        "node": node_data,
        "relations": relations
    }

@app.post("/nodes")
async def create_node_and_relationships(node_data: dict = None, token: str = Depends(token_is_valid)):
    """
    Создать узел и связи
    пример
    {
    "id": 11111111,
    "label": "User",
    "name": "Chel Chelikovich",
    "sex": 1,
    "city": "Тюмень",
    "screen_name": "chelikkkkk_chel",
    "follows": [11111111, 506443521],
    "subscribed": [178256900, 34491914]
    }
    """
    if not node_data:
        raise HTTPException(status_code=400, detail="No node data provided")
    
    label = node_data.get("label", "User")

    create_node_query = f"""
    CREATE (u:{label} {{id: $id, label: $label, name: $name, 
                        sex: $sex, city: $city, screen_name: $screen_name}})
    RETURN u
    """
    neo4j_handler.run_query(create_node_query, node_data)

    if "follows" in node_data:
        for follow_id in node_data["follows"]:
            follow_query = """
            MATCH (u:User {id: $id}), (f:User {id: $follow_id})
            CREATE (u)-[:FOLLOWS]->(f)
            """
            neo4j_handler.run_query(follow_query, {"id": node_data["id"], "follow_id": follow_id})

    if "subscribes" in node_data:
        for subscribe_id in node_data["subscribed"]:
            subscribe_query = """
            MATCH (u:User {id: $id}), (s:User {id: $subscribe_id})
            CREATE (u)-[:SUBSCRIBED]->(s)
            """
            neo4j_handler.run_query(subscribe_query, {"id": node_data["id"], "subscribe_id": subscribe_id})

    return {"status": "success"}



@app.delete("/nodes/{label}/{node_id}")
async def delete_node_and_relations(label: str, node_id: int, token: str = Depends(token_is_valid)):
    """
    Удаление узла и всех его связей по метке label и id.
    """
    query = f"""
    MATCH (n:{label} {{id: $node_id}})-[r]->()
    DELETE r
    """
    neo4j_handler.run_query(query, {"node_id": node_id})

    query = f"""
    MATCH (n:{label} {{id: $node_id}})<-[r]-()
    DELETE r
    """
    neo4j_handler.run_query(query, {"node_id": node_id})

    query = f"""
    MATCH (n:{label} {{id: $node_id}})
    DELETE n
    """
    neo4j_handler.run_query(query, {"node_id": node_id})

    return {"status": "success"}


if __name__ == "__main__":
    import uvicorn
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", type=str, default="neo4j://localhost:7687", help="URI подключения к Neo4j")
    parser.add_argument("--user", type=str, default="neo4j", help="Имя пользователя Neo4j")
    parser.add_argument("--password", type=str, required=True, help="Пароль пользователя Neo4j")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Хост для запуска")
    parser.add_argument("--port", type=int, default=8000, help="Порт для запуска")
    parser.add_argument("--token", type=str, required=True, help="Токен авторизации")

    args = parser.parse_args()
    SECRET_TOKEN = args.token
    neo4j_handler = Neo4jHandler(uri=args.uri, user=args.user, password=args.password)

    uvicorn.run(app, host=args.host, port=args.port)
