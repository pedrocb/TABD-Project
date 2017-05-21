import psycopg2 as psy
import sys
from matplotlib import pyplot as plt

def convert_results_to_lists(result):
    return map(list, zip(*result))

def plot_by_weekdays(conn):
    days = ["Domingo", "Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado"]
    cur = conn.cursor()
    cur.execute("select extract('dow' from timestamp) as dow, sum(n_services_initial) from timestamps, facts where timestamps.id = time group by dow order by dow;")
    results = cur.fetchall()
    lists = convert_results_to_lists(results)
    xs = lists[0]
    ys = lists[1]
    fig = plt.figure()
    plt.bar(xs, ys, align='center', alpha=0.5)
    plt.xticks(xs, days)

    plt.show()


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Wrong number of arguments")
        exit(0)

    user = sys.argv[1]
    dbname = sys.argv[2]
    conn = psy.connect("dbname=%s user=%s" % (dbname, user))

    plot_by_weekdays(conn)
