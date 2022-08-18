from neo4j import GraphDatabase

class NeoConnector:
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def excecute_transaction(self, query):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_and_return_greeting, query)
            print(result)
    
    @staticmethod
    def _run_transaction(tx, query):
        result = tx.run(query)
        return result.single()[0]



class HelloWorldExample:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

"""
if __name__ == "__main__":
    greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "admin")
    greeter.print_greeting("hello, world")
    greeter.close()
"""