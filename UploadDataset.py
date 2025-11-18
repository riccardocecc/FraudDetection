from neo4j import GraphDatabase
import pandas as pd
import time
import matplotlib.pyplot as plt
import seaborn as sns
import os
URI = "bolt://localhost:7687"  # Cambia se il tuo server è remoto
AUTH = ("neo4j", "12345678")  # Sostituisci con la tua password


def load_data(folder: str):

    loading_times = {}


    customers = pd.read_pickle(f"./data/{folder}/customers.pkl")
    terminals = pd.read_pickle(f"./data/{folder}/terminals.pkl")
    transactions = pd.read_pickle(f"./data/{folder}/transactions.pkl")

    transactions.to_csv(f"./data/{folder}/transactions.csv", index=False)
    transactions = pd.read_csv(
        f"./data/{folder}/transactions.csv")

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        try:
            driver.verify_connectivity()
            print("Connection established.")

            start_time = time.time()

            driver.execute_query(
                "CREATE CONSTRAINT customer_id_unique IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;")
            driver.execute_query(
                "CREATE CONSTRAINT terminal_id_unique IF NOT EXISTS FOR (t:Terminal) REQUIRE t.terminal_id IS UNIQUE;")
            driver.execute_query("CREATE INDEX amount_id_index  IF NOT EXISTS FOR (m:MADE_TRANSACTION) ON (m.amount)")
            driver.execute_query("CREATE INDEX date_index  IF NOT EXISTS FOR (m:MADE_TRANSACTION) ON (m.datetime)")
            driver.execute_query("CREATE INDEX nb_terminals_index  IF NOT EXISTS FOR (c:Customer) ON (c.nb_terminals)")


            end_time = time.time()
            print(f"Constraint e indici creati in {end_time - start_time:.2f} secondi")

            # Creazione dei nodi Customers
            start_time = time.time()
            customer_data = customers.to_dict(orient="records")
            driver.execute_query(
                """
                UNWIND $customers AS customer
                MERGE (c:Customer {customer_id: customer.CUSTOMER_ID})
                SET c.x = customer.x_customer_id, 
                    c.y = customer.y_customer_id, 
                    c.mean_amount = customer.mean_amount,
                    c.std_amount = customer.std_amount, 
                    c.mean_nb_tx_per_day = customer.mean_nb_tx_per_day,
                    c.nb_terminals = customer.nb_terminals
                """,
                customers=customer_data,
                database_="neo4j"
            )
            end_time = time.time()
            customers_time = end_time - start_time
            loading_times["Nodi customer"] = customers_time

            print(f"Customers caricati in {customers_time:.2f} secondi")

            # Creazione dei nodi Terminals
            start_time = time.time()
            terminal_data = terminals.to_dict(orient="records")
            driver.execute_query(
                """
                UNWIND $terminals AS terminal
                MERGE (t:Terminal {terminal_id: terminal.TERMINAL_ID})
                SET t.x = terminal.x_terminal_id, t.y = terminal.y_terminal_id
                """,
                terminals=terminal_data,
                database_="neo4j"
            )
            end_time = time.time()
            terminals_time = end_time - start_time
            loading_times["Nodi Terminal"] = terminals_time
            print(f"Terminals caricati in {terminals_time:.2f} secondi")

            # Creazione delle relazioni "available_terminals" tra Customers e Terminals
            start_time = time.time()
            customer_terminals_data = [
                {"customer_id": row["CUSTOMER_ID"], "available_terminals": row["available_terminals"]}
                for _, row in customers.iterrows() if isinstance(row["available_terminals"], list)
            ]

            driver.execute_query(
                """
                UNWIND $data AS entry
                MATCH (c:Customer {customer_id: entry.customer_id})
                UNWIND entry.available_terminals AS terminal_id
                MATCH (t:Terminal {terminal_id: terminal_id})
                MERGE (c)-[:AVAILABLE_TERMINALS]->(t)
                """,
                data=customer_terminals_data,
                database_="neo4j"
            )

            end_time = time.time()
            available_terminals_time = end_time - start_time
            loading_times["AVAILABLE_TERMINALS"] = available_terminals_time
            print(f"Relazioni AVAILABLE_TERMINALS create in {available_terminals_time:.2f} secondi")

            # Creazione delle transazioni
            def run_query(query):
                with driver.session() as session:
                    result = session.run(query)
                    return result.single()[0]

            csv_path = os.path.abspath(f"./data/{folder}/transactions.csv")  # Percorso assoluto
            neo4j_url = f"file:///{csv_path.replace(os.sep, '/')}"  # Convertire per Neo4j: usa "/" e aggiungi il protocollo
            query = f"""
                LOAD CSV WITH HEADERS FROM '{neo4j_url}' AS line
                CALL(line) {{
                    MATCH (c:Customer {{customer_id: toInteger(line.CUSTOMER_ID)}})
                    MATCH (t:Terminal {{terminal_id: toInteger(line.TERMINAL_ID)}})
                    CREATE (c)-[:MADE_TRANSACTION {{
                        id: toInteger(line.TRANSACTION_ID),
                        datetime: date(substring(line.TX_DATETIME, 0, 10)),
                        amount: toFloat(line.TX_AMOUNT),
                        time_seconds: toInteger(line.TX_TIME_SECONDS),
                        time_days: toInteger(line.TX_TIME_DAYS),
                        fraud: toBoolean(line.TX_FRAUD),
                        fraud_scenario: toInteger(line.TX_FRAUD_SCENARIO)
                    }}]->(t)
                }} IN TRANSACTIONS OF 10000 ROWS
                RETURN count(*) AS relazioni_create
            """

            print("Caricamento transazioni...")
            start_time = time.time()
            relazioni_create = run_query(query)
            end_time = time.time()
            transactions_time = end_time - start_time
            loading_times["MADE_TRANSACTION"] = transactions_time
            print(
                f"{relazioni_create} relazioni TRANSACTIONS create con successo in {transactions_time:.2f} secondi")
            plot_execution_times(loading_times, folder, relazioni_create)
        except Exception as e:
            print(f"Errore: {e}")

def plot_execution_times(execution_times: dict, folder:str, relazioni_create:str):
    operations = list(execution_times.keys())
    times = list(execution_times.values())
    plt.figure(figsize=(8, 5))
    bars = plt.bar(operations, times)
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 0.01, f"{height:.2f}s",
                 ha='center', va='bottom')
    plt.title(f"Tempi di Caricamento {folder} - {relazioni_create} transazioni")
    plt.xlabel("Operazione")
    plt.ylabel("Time (s)")
    plt.tight_layout()
    output_dir = f"./data/{folder}/output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/loading_times_chart_{folder}.png"
    plt.savefig(output_path)


