from neo4j import GraphDatabase

from Streaming_Architekture import env_vars
from Streaming_Architekture.general.models import Tweet, User, Relationship


class Neo4JHelper:

    def __init__(self):
        self.driver = GraphDatabase.driver(env_vars.DB_HOST, auth=(env_vars.DB_USER, env_vars.DB_PASSWORD))
        self.create_indexes()

    def create_indexes(self):
        query1 = "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.screen_name)"
        query2 = "CREATE INDEX IF NOT EXISTS FOR (r:CommentedAt) ON (r.rel_id)"
        query3 = "CREATE INDEX IF NOT EXISTS FOR (h:Hashtag) ON (h.tag)"
        query4 = "CREATE INDEX IF NOT EXISTS FOR (t:Tweet) ON (t.id)"

        with self.driver.session() as session:
            session.run(query1)
            session.run(query2)
            session.run(query3)
            session.run(query4)

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

    def get_user(self, screen_name):
        query = """
            MATCH (u:User{screen_name: "%s"})
            RETURN u
        """ % screen_name

        with self.driver.session() as session:
            result = session.run(query)
            return result.single()

    def create_hashtag(self, user: User, hashtag: str):
        query = """
            MATCH (u:User{screen_name: "%s"})
            MERGE (h:Hashtag{tag: "%s"})
            ON CREATE SET h.count= 1
            ON MATCH SET h.count= h.count+1
            MERGE (u)-[r:usedHashtag]->(h)
            ON CREATE SET r.weight= 1
            ON MATCH SET r.weight= r.weight+1
            RETURN r
        """ % (user.screen_name, hashtag)

        with self.driver.session() as session:
            result = session.run(query)
            return result.single()

    def create_tweet(self, tweet: Tweet):
        query = """
            MATCH (u:User {screen_name: "%s"})
            CREATE (t:Tweet{
                id : "%s",
                screen_name: "%s",
                user_id: "%s",
                polarity: %s,
                created_at: "%s",
                hashtags: %s,
                full_test: "%s"
            })
            CREATE (u)-[r:wrote]->(t)
        """ % (tweet.user.screen_name,
               tweet.id,
               tweet.user.screen_name,
               tweet.user.user_id,
               tweet.sentiment,
               tweet.created_at,
               ("[]" if tweet.hashtags is None else tweet.hashtags),
               tweet.full_text.replace("\"", "'")
               )

        with self.driver.session() as session:
            result = session.run(query)
            return result.single()

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
                    u.tweet_count = 1,
                    u.type = "%s"
            ON MATCH
                SET u.tweet_count = %s + u.tweet_count
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
            if result is None:
                return None

            return result.graph()

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
