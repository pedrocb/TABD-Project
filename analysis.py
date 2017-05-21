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

def plot_feriados(conn):
    feriados = [[1,1], [3,4], [5,4], [25,4], [1,5], [4,6], [10,6], [15, 8]]
    feriadosString = ("1 de Janeiro", "3 de Abril", "5 de Abril", "25 de Abril", "1 de Maio", "4 de Junho", "10 de Junho", "15 de Agosto")
    count = []
    cur = conn.cursor()
    for feriado in feriados:
        cur.execute("select sum(n_services_initial) from facts, timestamps where time = timestamps.id and extract('day' from timestamp) = %d and extract('month' from timestamp) = %d" % (feriado[0], feriado[1]))
        count.append(convert_results_to_lists(cur.fetchall())[0][0])
        print(count)
    fig = plt.figure()
    xs = (1,2,3,4,5,6,7,8)
    plt.bar(xs, count)
    plt.xticks(xs, feriadosString)
    plt.show()
    print("Media: ", sum(count)/float(len(count)))




if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Wrong number of arguments")
        exit(0)

    user = sys.argv[1]
    dbname = sys.argv[2]
    conn = psy.connect("dbname=%s user=%s" % (dbname, user))

    plot_by_weekdays(conn)
    plot_feriados(conn)
