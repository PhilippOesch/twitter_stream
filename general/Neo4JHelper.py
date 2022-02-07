import logging

from neo4j import GraphDatabase

import env_vars
from general import utils
from general.project_dataclasses import Tweet, User, Relationship


class Neo4JHelper:

    def __init__(self):
        self.driver = GraphDatabase.driver(env_vars.DB_HOST, auth=(env_vars.DB_USER, env_vars.DB_PASSWORD))
        self.create_indexes()

    def create_indexes(self):
        query1 = """
            CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.screen_name)
        """

        query2 = """            
            CREATE INDEX IF NOT EXISTS FOR (r:CommentedAt) ON (r.rel_id)
        """

        with self.driver.session() as session:
            session.run(query1)
            session.run(query2)

    def reset(self):
        query = """
            MATCH (n)
            DETACH DELETE n
        """

        with self.driver.session() as session:
            session.run(query)

        print("DB cleared")

    def close(self):
        self.driver.close()

    def create_or_merge_user(self, user: User):
        if user.tweet_count is None:
            tweet_count = 1
        else:
            tweet_count = user.tweet_count

        query = """
            MERGE (u:User {screen_name: "%s"})
            ON CREATE
                SET 
                    u.id = %s,
                    u.tweet_count = 1
                    u.type = %s
            ON MATCH
                SET u.tweet_count = %s
            RETURN u.screen_name as screen_name
        """ % (user.screen_name, user.user_id, user.type, tweet_count)

        with self.driver.session() as session:
            result = session.run(query)
            return result.single()

        # print("User added")

    def get_relationship(self, start_user: str, end_user: str) -> any:
        query = """
            MATCH (user1:User{screen_name:"%s"})-[r:CommentedAt]->(user2:User{screen_name:"%s"})
            RETURN r, user1, user2
        """ % (start_user, end_user)

        with self.driver.session() as session:
            result = session.run(query)
            return [record for record in result]

    def create_or_merge_relationship(self, rel: Relationship) -> any:
        query = """
            MATCH
                (user1:User {screen_name: "%s"}),
                (user2:User {screen_name: "%s"})
            MERGE (user1)-[r:CommentedAt{rel_id: "%s"}]->(user2)
            SET
                r.weight = %s,
                r.avg_polarity = %s,
                r.weighted_polarity= %s
            RETURN r, user1, user2
        """ % (rel.start_user, rel.end_user, rel.id, rel.weight, rel.avg_polarity, rel.weighted_polarity)

        with self.driver.session() as session:
            result = session.run(query)
            return [record for record in result]
