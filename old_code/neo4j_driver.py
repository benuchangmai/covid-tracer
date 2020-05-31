from neo4j import GraphDatabase
import time
from dateutil.parser import parse

def create_person(tx, uid):
    return tx.run("CREATE (a:User {id:$id}) "
                  "RETURN id(a)", id=int(uid)).single().value()

def create_location(tx, loc):
    return tx.run("CREATE (a:Location {loc:$loc}) "
                  "RETURN id(a)", loc=loc).single().value()

def find_person(tx, uid):
    result = tx.run("MATCH (n:User) WHERE n.id = $id "
                  "RETURN id(n)", id=int(uid)).single()
    if result is None:
        return None
    return result.value()

def find_location(tx, loc):
    result = tx.run("MATCH (n:Location) WHERE n.loc = $loc "
                  "RETURN id(n)", loc=loc).single()
    if result is None:
        return None
    return result.value()

def add_relation(tx, uid, loc, timestamp):
    dt = timestamp
    dtime = parse(dt)
    dtime = time.mktime(dtime.timetuple())
    return tx.run("MATCH (a:User),(b:Location) "
                  "WHERE a.id = $id AND b.loc = $loc "
                  "MERGE (a)-[r:VISITED { time: $time }]->(b) "
                  "RETURN type(r)", id=int(uid), loc=loc, time=int(dtime)).single().value()

def delete_all_nodes_rel(tx):
    return tx.run('match (a) -[r] - () delete a, r')

def delete_all_nodes(tx):
    return tx.run('match (a) delete a')

def find_neighbors_count(tx, uid, nhops, infected_timestamp):
    return tx.run("MATCH (u:User{id:$id})-[r1]->()<-[r2]-(c:User) WHERE abs(r1.time-r2.time) < 7200 "
        "AND (r1.time-$infTime) < 1209600 "
        "RETURN COUNT(DISTINCT c)", id=int(uid), infTime=int(infected_timestamp), nhops=nhops)

def find_neighbors(tx, uid, nhops, infected_timestamp):
    return tx.run("MATCH (u:User{id:$id})-[r1]->()<-[r2]-(c:User) WHERE abs(r1.time-r2.time) < 7200 "
        "AND (r1.time-$infTime) < 1209600 "
        "RETURN DISTINCT c", id=int(uid), infTime=int(infected_timestamp), nhops=nhops)

def find_neighbors_next(tx, uid, nhops, infected_timestamp):
    return tx.run("MATCH (u:User{id:$id})-[r1]->()<-[r2]-(b:User)-[r3]->()<-[r4]-(c:User) "
        "WHERE abs(r1.time-r2.time) < 7200 "
        "AND abs(r3.time-r4.time) < 7200 "
        "AND (r1.time-$infTime) < 1209600 "
        "AND (r3.time-$infTime) < 2419200 "
        "RETURN DISTINCT c", id=int(uid), infTime=int(infected_timestamp), nhops=nhops)


class DBHelper():
    def __init__(self, uri, password):
        self.driver = GraphDatabase.driver(uri, auth=('neo4j', password), encrypted=True)

    def create_user(self, uid):
        session = self.driver.session()
        uid = session.write_transaction(create_person, uid)
    
    def create_location(self, loc):
        session = self.driver.session()
        uid = session.write_transaction(create_location, loc)

    def reset(self):
        session = self.driver.session()
        session.write_transaction(delete_all_nodes_rel)
        session.write_transaction(delete_all_nodes)
        
    def user_exists(self, uid):
        session = self.driver.session()
        return session.write_transaction(find_person, uid)

    def location_exists(self, loc):
        session = self.driver.session()
        return session.write_transaction(find_person, loc)

    def add_relationship(self, uid, loc, time):
        session = self.driver.session()
        session.write_transaction(add_relation, uid, loc, time)

    def find_neighbors(self, uid, nhops, infected_timestamp):
        session = self.driver.session()
        return session.read_transaction(find_neighbors, uid, nhops, infected_timestamp)

    def find_neighbors_count(self, uid, nhops, infected_timestamp):
        session = self.driver.session()
        return session.read_transaction(find_neighbors_count, uid, nhops, infected_timestamp)

    def find_neighbors_next(self, uid, nhops, infected_timestamp):
        session = self.driver.session()
        return session.read_transaction(find_neighbors_next, uid, nhops, infected_timestamp)


if __name__ == "__main__":
    #uri = "bolt://localhost:7687"
    #url = "bolt://34-68-238-91.gcp-neo4j-sandbox.com:7687"
    uri = "bolt://34.94.143.242:7687"
    password = "Siso@123"
    #password = 'siso@123'
    #pwd = "MfGCmB2upGq4ku7F"
    db = DBHelper(uri, password)
    db.reset()