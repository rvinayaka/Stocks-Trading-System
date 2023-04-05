from flask import Flask, request, jsonify
from conn import connection
from settings import logger
import psycopg2

app = Flask(__name__)


# Question
# Stock trading system - Design a class to manage stock trades,
# including buying and selling stocks, calculating profits, and losses.

# Query
# CREATE TABLE stocks(sno SERIAL PRIMARY KEY, stock_name VARCHAR(200) NOT NULL ,
# status VARCHAR(200), profits NUMERIC, losses NUMERIC, balance NUMERIC);

# Table
#  sno | stock_name  | status  | returns | balance | calculated
# -----+-------------+---------+---------+---------+------------
#    1 | Adani Power | selling |   -2000 |    2800 | t
#    2 | Tata motors | selling |     150 |    3000 | t
#    3 | Synergy     | buying  |     150 |    1800 | f
#    4 | ITC         | buying  |      80 |    1800 | t
#    5 | sysco       | buying  |     200 |    1200 |
#    6 | ABCD        | buying  |     100 |    1000 |




def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.Error as error:
            conn = kwargs.get('conn')
            if conn:
                conn.rollback()
            logger(__name__).error(f"Error occurred: {error}")
            return jsonify({"message": f"Error occurred: {error}"})
        except Exception as error:
            logger(__name__).error(f"Error occurred: {error}")
            return jsonify({"message": f"Error occurred: {error}"})
        finally:
            conn = kwargs.get("conn")
            cur = kwargs.get("cur")
            # close the database connection
            if conn:
                conn.close()
            if cur:
                cur.close()
            logger(__name__).warning("Closing the connection")
    return wrapper


@app.route("/stocks", methods=["POST"])             # CREATE a stock profile
@handle_exceptions
def add_new_stock():
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to add new stocks")

    stock_name = request.json["stockName"]
    status = request.json["status"]
    returns = request.json["returns"]
    balance = request.json["balance"]
    # format = {
    #     "stockName": "sysco",
    #     "status": "buying",
    #     "returns": 200,
    #     "balance": 1200
    # }

    add_query = """INSERT INTO stocks(stock_name, status,  
                                    returns, balance) VALUES (%s, %s, %s, %s)"""

    values = (stock_name, status, returns, balance)
    cur.execute(add_query, values)
    conn.commit()

    logger(__name__).info(f"{stock_name} added in the list")
    return jsonify({"message": f"{stock_name} added in the list"}), 200

@app.route("/", methods=["GET"], endpoint='show_all_stocks')                            # READ the details of all stocks
@handle_exceptions
def show_all_stocks():
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to display stocks in the list")

    cur.execute("SELECT * FROM stocks")
    data = cur.fetchall()
    # Log the details into logger file
    logger(__name__).info("Displayed list of all stocks in the list")
    return jsonify({"message": data}), 200


@app.route("/report/<int:sno>", methods=["GET"], endpoint='get_stock_report')
@handle_exceptions
def get_stock_report(sno):
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to display stock's report card")

    show_query = "SELECT * FROM stocks WHERE sno = %s;"
    cur.execute(show_query, (sno,))
    data = cur.fetchone()

    # Log the details into logger file
    logger(__name__).info(f"Displayed report card of stocks mo. {sno}")
    return jsonify({"message": data}), 200


@app.route("/search/<string:stock_name>", methods=["GET"], endpoint='search_stock')
@handle_exceptions
def search_stock(stock_name):
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to display stock's report card")

    show_query = "SELECT * FROM stocks WHERE stock_name = %s;"
    cur.execute(show_query, (stock_name,))
    data = cur.fetchone()

    # Log the details into logger file
    logger(__name__).info(f"Displayed stocks no. {stock_name}")
    return jsonify({"message": data}), 200


@app.route("/transactions", methods=["GET"], endpoint='view_transaction_history')
@handle_exceptions
def view_transaction_history():
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to display transaction history")

    show_query = "SELECT returns, balance FROM stocks;"
    cur.execute(show_query)
    data = cur.fetchall()

    # Log the details into logger file
    logger(__name__).info(f"Displayed transaction history")
    return jsonify({"message": data}), 200

@app.route("/stocks/<int:sno>", methods=["PUT"], endpoint='update_stock_details')  # update details of stocks
@handle_exceptions
def update_stock_details(sno):
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to update the details ")

    cur.execute("SELECT stock_name from stocks where sno = %s", (sno,))
    get_character = cur.fetchone()

    if not get_character:
        return jsonify({"message": "Character not found"}), 200
    data = request.get_json()
    stock_name = data.get('stock_name')
    status = data.get('status')
    returns = data.get('returns')
    balance = data.get('balance')

    if stock_name:
        cur.execute("UPDATE game SET stock_name = %s WHERE sno = %s", (stock_name, sno))
    elif status:
        cur.execute("UPDATE game SET status = %s WHERE sno = %s", (status, sno))
    elif returns:
        cur.execute("UPDATE game SET returns = %s WHERE sno = %s", (returns, sno))
    elif balance:
        cur.execute("UPDATE game SET balance = %s WHERE sno = %s", (balance, sno))

    conn.commit()
    # Log the details into logger file
    logger(__name__).info(f"Stocks details updated: {data}")
    return jsonify({"message": "stocks details updated", "Details": data}), 200

@app.route("/returns/<string:stock_name>", methods=["GET", 'PUT'], endpoint='calc_returns')
@handle_exceptions
def calc_returns(stock_name):   # calculate returns of each stock
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to display stock's returns")

    get_query = "SELECT status, balance from stocks WHERE stock_name = %s"
    values = (stock_name,)

    cur.execute(get_query, values)
    get_balance = cur.fetchall()

    status = get_balance[0][0]
    balance = get_balance[0][1]

    print(get_balance, status, balance)

    if request.method == "PUT":
        if status == "buying":
            returns = request.json["returns"]
            updated_bal = balance + returns

            query = """UPDATE stocks SET balance = %s WHERE stock_name = %s"""
            values = (updated_bal, stock_name)

            cur.execute(query, values)
            conn.commit()

    # Log the details into logger file
    logger(__name__).info(f"Displayed returns of {stock_name}")
    return jsonify({"message": f"Displayed returns of stocks no. {stock_name}"}), 200


@app.route("/delete/<int:sno>", methods=["DELETE"], endpoint='delete_stock')      # DELETE an item from cart
@handle_exceptions
def delete_stock(sno):
    # start the database connection
    cur, conn = connection()
    logger(__name__).warning("Starting the db connection to delete stocks from the list")

    delete_query = "DELETE from stocks WHERE sno = %s"
    cur.execute(delete_query, (sno,))
    conn.commit()
    # Log the details into logger file
    logger(__name__).info(f"Stocks with sno no. {sno} deleted from the table")
    return jsonify({"message": "Deleted Successfully", "char no": sno}), 200




if __name__ == "__main__":
    app.run(debug=True, port=5000)
