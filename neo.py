from neo4j import GraphDatabase

class Staff:

    # lists of names, ages and gender that match to the data in the indexes:
    names = ["Chandler", "Monica", "Rachel", "Emma", "Ross", "Phebe", "joey", "Rosalinda", "Paz"]
    ages = [32, 32, 32, 1, 34, 31, 32, 32, 32]
    genders = ["Male", "Female", "Female", "Female", "Male", "Female", "Male", "Female", "Male"]

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def __session(self):
        with self.driver.session() as session:
            return session

    def create(self):
        tx = self.__session()
        for i in range(len(self.names)):
            create = "CREATE (p:Person {name:'" + str(self.names[i]) + "', Age:" + str(self.ages[i]) + ", gender:'" + str(self.genders[i]) + "'}) "
            tx.run(str(create))
        for i in range(19, 21):
            create = "CREATE (a:Apartment {number:" + str(i) + "}) "
            tx.run(str(create))

    def match(self):
        tx = self.__session()
        matchs = ['''MATCH (p:Person),(a:Apartment)
                     WHERE p.Age = 32 AND p.gender = 'Female' AND a.number = 20
                     CREATE (p)-[r:Lives_in]->(a)     
                  ''',
                  '''MATCH (p:Person),(a:Apartment)
                     WHERE p.Age = 32 AND p.gender = 'Male' AND a.number = 19
                     CREATE (p)-[r:Lives_in]->(a)     
                  ''',
                  '''MATCH (p:Person)
                     WHERE p.Age <> 32 AND p.Age <> 1
                     CREATE (p)-[:Lives_in]->(a:Apartment)      
                  ''',
                  '''MATCH (p:Person),(p1:Person),(p2:Person)
                     WHERE p.name = 'Rachel' AND p1.Age = 34 AND p2.Age = 1
                     CREATE (p)-[pa:Parent_of]->(p2)<-[:Parent_of]-(p1)  
                  ''',
                  '''MATCH (p:Person),(p1:Person),(p2:Person)
                     WHERE p.name = 'Ross' AND p1.name = 'Monica' AND p2.name = 'Chandler'
                     CREATE (p)-[s:Sibling]->(p1)
                     CREATE (p1)-[:Sibling]->(p)
                     CREATE (p2)-[m:Married_to]->(p1)
                     CREATE (p1)-[:Married_to]->(p2)
                  '''
                  ]
        for match in matchs:
            tx.run(str(match))

    def query(self, query):
        tx = self.__session()
        return tx.run(str(query)).data()


if __name__ == "__main__":
    staff = Staff("bolt://localhost:7687", "neo4j", "511")
    staff.create()
    staff.match()

    queries = {"ORDER BY AGE IN DESCENDING ORDER:": '''MATCH(p:Person)
               RETURN p.name as name, p.Age as age
               ORDER BY age DESC
               ''',
               "THE MALE:": '''MATCH(p:Person)
               WHERE p.gender='Male'
               RETURN p.name as name             
               ''',
               "RELATION": '''
               MATCH (p:Person)-[r]->(otherPerson:Person)
               WITH *, type(r) as relationship
               RETURN p.name as person, otherPerson.name as otherPerson, relationship             
               '''
               }
    for q in queries.keys():
        data = staff.query(queries[q])
        print(q)
        for d in data:
            for k in d.keys():
                print(str(k) + " : " + str(d[k]), end="    ")
            print("\n")
    staff.close()

