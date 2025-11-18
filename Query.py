# query.py
from neo4j import GraphDatabase
import time
import matplotlib.pyplot as plt
import os
import csv
# Configurazione della connessione
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "12345678")


query_SimilarSpenders="""

MATCH (c:Customer)-[tx:MADE_TRANSACTION]->(t:Terminal)
WHERE tx.amount IS NOT NULL
WITH c.customer_id AS custId, t.terminal_id AS termId, sum(tx.amount) AS totalAmt
WITH {cust: custId, term: termId, amt: totalAmt} AS entry


WITH entry.term AS termId, collect(entry) AS entries

UNWIND entries AS a
UNWIND entries AS b
WITH a, b
WHERE a.cust < b.cust   // Evita duplicati e sé stessi


WITH 
  a.cust AS c1,
  b.cust AS c2,
  collect({
    term: a.term,
    amt1: a.amt,
    amt2: b.amt
  }) AS sharedTerms


WHERE size(sharedTerms) >= 3


WITH 
  c1, c2, sharedTerms,
  reduce(sum1 = 0.0, x IN sharedTerms | sum1 + x.amt1) AS total1,
  reduce(sum2 = 0.0, x IN sharedTerms | sum2 + x.amt2) AS total2


WHERE abs(total1 - total2) <= total1 * 0.1


RETURN 
  c1 AS customer_x,
  total1 AS total_spent_x,
  c2 AS customer_y,
  total2 AS total_spent_y,
  size(sharedTerms) AS common_terminals,
  [x IN sharedTerms | x.term] AS shared_terminal_ids;
"""

query_PossibileFraudulentTansactions ="""
CALL {
  WITH date("2018-04-01") AS prev_month_start, date("2018-05-01") AS curr_month_start
  MATCH (:Customer)-[tx:MADE_TRANSACTION]->(t:Terminal)
  WHERE tx.datetime >= prev_month_start AND tx.datetime < curr_month_start
  WITH t.terminal_id AS terminal_id, avg(tx.amount) AS avg_amount
  RETURN apoc.map.fromPairs(collect([terminal_id, avg_amount])) AS terminal_avg_map
}
WITH terminal_avg_map, date("2018-05-01") AS curr_month_start, date("2018-06-01") AS curr_month_end
MATCH (c:Customer)-[tx:MADE_TRANSACTION]->(t:Terminal)
WHERE tx.datetime >= curr_month_start AND tx.datetime < curr_month_end

WITH 
  c.customer_id AS CustomerID,
  t.terminal_id AS TerminalID,
  tx.amount AS FraudulentAmount,
  toString(tx.datetime) AS TransactionDate,
  terminal_avg_map,
  toFloat(terminal_avg_map[toString(t.terminal_id)]) AS avg_prev_month

WHERE FraudulentAmount > 1.2 * avg_prev_month

RETURN 
  TerminalID,
  CustomerID,
  FraudulentAmount,
  avg_prev_month AS PreviousMonthAverage,
  TransactionDate
"""


k_value = 3
customer_id = 6
query_CoCustomersK = f"""//
    WITH {(k_value-1) * 2} AS k
    MATCH (start:Customer {{customer_id: {customer_id}}})
        CALL apoc.path.expandConfig(start, {{
        relationshipFilter: 'MADE_TRANSACTION',
        labelFilter: 'Terminal|Customer',
        maxLevel: k,
        uniqueness: 'NODE_GLOBAL'
    }}) YIELD path
    WITH path
    WHERE length(path) = k
    RETURN nodes(path)[-1].customer_id AS co_customer
"""


extends_buyingF = """
   
    CALL apoc.periodic.iterate(
      "
        MATCH (c:Customer)-[r:MADE_TRANSACTION]->(t:Terminal)
        RETURN r
      ",
      "
        WITH r,
             CASE
               WHEN r.time_seconds >= 6*3600  AND r.time_seconds < 12*3600 THEN 'morning'
               WHEN r.time_seconds >= 12*3600 AND r.time_seconds < 18*3600 THEN 'afternoon'
               WHEN r.time_seconds >= 18*3600 AND r.time_seconds < 24*3600 THEN 'evening'
               ELSE 'night'
             END AS period,
             ['high-tech','food','clothing','consumable','other'][toInteger(rand()*5)] AS product,
             toInteger(rand()*5 + 1) AS feeling
        SET r.period_of_day   = period,
            r.product_type    = product,
            r.security_feeling = feeling
      ",
      {batchSize:10000, parallel:true}
    )
    YIELD batches AS b1, total AS t1
    
      
    
    CALL apoc.periodic.iterate(
      "
        MATCH (c:Customer)-[mt:MADE_TRANSACTION]->(t:Terminal)
        WITH t.terminal_id AS tid, c, 
             count(mt) AS txCount, 
             avg(mt.security_feeling) AS avgSec
        WHERE txCount > 3
        RETURN tid, c, avgSec
      ",
      "
        MATCH (t:Terminal {terminal_id: tid})
        WITH t, collect({cust: c, sec: avgSec}) AS custs
        UNWIND apoc.coll.combinations(custs, 2) AS pair
        WITH pair[0].cust AS cA, pair[1].cust AS cB,
             pair[0].sec  AS sA, pair[1].sec  AS sB
        WHERE abs(sA - sB) < 1
        MERGE (cA)-[:BUYING_FRIENDS]->(cB)
      ",
      {batchSize:500, parallel:true}
    )
    YIELD batches AS b2, total AS t2
    
    
    RETURN 
      "estensione transazioni"  AS step1, b1 AS batches1, t1 AS total1,
      "creazione buying friends" AS step2, b2 AS batches2, t2 AS total2;
    """

create_period_index = """
CREATE INDEX period_day_index  IF NOT EXISTS FOR (m:MADE_TRANSACTION) ON (m.period_of_day)
"""


query_PeriodFraudStats = """
MATCH ()-[mt:MADE_TRANSACTION]->()
WITH mt.period_of_day AS timePeriod,
     count(*) AS totalTx,
     sum(CASE WHEN mt.fraud_scenario > 0 THEN 1 ELSE 0 END) AS fraudTx,
     avg(CASE WHEN mt.fraud_scenario > 0 THEN 1.0 ELSE 0.0 END) AS avgFraud
RETURN
  timePeriod,
  totalTx,
  fraudTx,
  avgFraud
"""

queries = {
    "SimilarSpenders": query_SimilarSpenders,
    "FraudulentTansactions": query_PossibileFraudulentTansactions,
    "CoCustomersK": query_CoCustomersK,
    "ExtendsBuyingF": extends_buyingF,
    "periodIndex": create_period_index,
    "PeriodFraudStats":query_PeriodFraudStats

}


def save_query_results_to_csv(name: str, folder: str, records):
    os.makedirs("QueryResult", exist_ok=True)
    filepath = os.path.join("QueryResult", f"{name}_{folder}.csv")
    if not records:
        print(f"No records to save for query {name}.")
        return

    keys = records[0].keys()
    with open(filepath, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for record in records[:30]:
            writer.writerow(record.data())
    print(f"Saved top 10 results of {name} to {filepath}")


def execute_query(driver, query, database="neo4j"):
    start_time = time.time()
    records, summary, _ = driver.execute_query(query, database_=database)
    duration = time.time() - start_time
    return records, summary, duration


def run_all_queries( folder: str,uri=URI, auth=AUTH, database="neo4j"):

    with GraphDatabase.driver(uri, auth=auth) as driver:
        driver.verify_connectivity()
        print("Connection established.")
        execution_times = {}
        record_counts = {}
        for name, query in queries.items():
            print(f"Running {name}...")
            records, summary, duration = execute_query(driver, query, database)
            execution_times[name] = duration
            record_counts[name] = len(records)
            print(f"{name} executed in {duration:.2f} s, {len(records)} records returned.")


            if name not in {"periodIndex"}:
                save_query_results_to_csv(name, folder, records)

        exclude_from_plot = {"Create_feeling_security_index", "periodIndex"}
        filtered_times = {k: v for k, v in execution_times.items() if k not in exclude_from_plot}
        print(filtered_times)

        plot_execution_times(filtered_times, folder, record_counts)



def plot_execution_times(execution_times: dict, folder: str, record_counts: dict):
    operations = list(execution_times.keys())
    times = list(execution_times.values())
    record_counts_list = [record_counts[op] for op in operations]

    plt.figure(figsize=(8, 5))
    bars = plt.bar(operations, times)


    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height + 0.01, f"{height:.2f}s\n{record_counts_list[i]} records",
                 ha='center', va='bottom')

    plt.title(f"Query execution time {folder}")
    plt.xlabel("Query")
    plt.ylabel("Time (s)")
    plt.tight_layout()
    plt.savefig(f"performance_queries_{folder}.png")
    print("Chart saved as performance_queries.png")



if __name__ == "__main__":
    run_all_queries()


