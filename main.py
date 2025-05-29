import mysql.connector
import time
from threading import Thread, Event

DB_NAME = "il_levels_demo"
DB_USER = "root"
DB_PASSWORD = "123456Aa"
DB_HOST = "localhost"
DB_PORT = 3306

ISOLATION_LEVELS = ("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE")

def get_connection(isolation_level_str: str):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        if isolation_level_str in ISOLATION_LEVELS:
            conn.autocommit = False
            conn.cmd_query(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level_str};")
            print(f"[INFO] Connection set to {isolation_level_str}.")
        else:
            raise ValueError(f"Unsupported isolation level: {isolation_level_str}")

        return conn

    except mysql.connector.Error as e:
        print(f"[ERROR] Could not connect to database: {e}")
        exit(1)


def fetch_balance(conn, account_holder: str, label: str = ""):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM accounts WHERE account_holder = %s", (account_holder,))
            balance = cur.fetchone()
            if balance:
                print(f"[{label}] {account_holder}'s balance: {balance[0]:.2f}")
            else:
                print(f"[{label}] {account_holder} not found.")
            return balance[0] if balance else None
    except mysql.connector.Error as e:
        print(f"[ERROR] Error fetching balance: {e}")
        return None


def update_balance(conn, account_holder: str, amount: float, label: str = ""):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE accounts SET balance = balance + %s WHERE account_holder = %s",
                (amount, account_holder)
            )
            print(f"[{label}] Updated {account_holder}'s balance by {amount:.2f}.")
    except mysql.connector.Error as e:
        print(f"[ERROR] Error updating balance: {e}")


def insert(conn, account_holder:str, amount: float, label: str = ""):
    try:
        if amount < 0:
            print(f"[ERROR {label}] Balance cannot be negative.")
            return None

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO accounts (account_holder, balance) VALUES (%s, %s)",
                (account_holder, amount)
            )

        print(f"[{label}] Inserted holder: {account_holder} with  balance: {amount}.")

    except mysql.connector.Error as e:
        print(f"[ERROR] Error fetching balance: {e}")
        return None


def sum_balance(conn, label: str = ""):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sum(balance) FROM accounts"
            )
            sum = cur.fetchone()[0]

            if sum:
                print(f"[{label}] Sum balance: {sum:.2f}")
            else:
                print(f"[{label}] Sum balance not found.")

            return sum if sum else None

    except mysql.connector.Error as e:
        print(f"[ERROR] Error fetching balance: {e}")
        return None

def count_accounts(conn, label: str = ""):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM accounts"
            )
            count = cur.fetchone()[0]

            if count:
                print(f"[{label}] {count}")
            else:
                print(f"[{label}] Count not found.")

            return count if count else None

    except mysql.connector.Error as e:
        print(f"[ERROR] Error fetching balance: {e}")
        return None




def reset_database():
    conn = get_connection("READ COMMITTED")
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE accounts")
            cur.execute("INSERT INTO accounts (account_holder, balance) VALUES (%s, %s)", ("Alice", "1000.0"))
            cur.execute("INSERT INTO accounts (account_holder, balance) VALUES (%s, %s)", ("Bob", "500.0"))
            conn.commit()
            print("\n[INFO] Database reset to initial state.")
    except mysql.connector.Error as e:
        print(f"[ERROR] Error resetting database: {e}")
        conn.rollback()
    finally:
        conn.close()

# Showing dirty read

def dirty_read():
    tx_1_update = Event()
    tx_2_read = Event()

    def transaction_1(conn_str):
        conn = get_connection(conn_str)
        try:
            print(f"\n[Tx1-{conn_str}] Starting transaction 1.")
            conn.start_transaction()

            update_balance(conn, "Alice", 100, f"Tx1-{conn_str}");
            print(f"\n[Tx1-{conn_str}] Alice balance updated. Not commited.")
            tx_1_update.set()

            tx_2_read.wait()

            conn.commit()
            print(f"\n[Tx1-{conn_str}] Transaction 1 commited.")

        except mysql.connector.Error as e:
            print(f"[ERROR] Error starting transaction 1: {e}")
            conn.rollback()
            print("Transaction 1 rollbacked")

        finally:
            conn.close()

    def transaction_2(conn_str):
        conn = get_connection(conn_str)

        try:
            print(f"\n[Tx2-{conn_str}] Starting transaction 2.")
            conn.start_transaction()

            tx_1_update.wait()

            fetch_balance(conn, "Alice", f"Tx2-{conn_str} (first read)")
            tx_2_read.set()

            time.sleep(1)

            fetch_balance(conn, "Alice", f"Tx2-{conn_str} (second read)")

        except mysql.connector.Error as e:
            print(f"[ERROR] Error starting transaction 2: {e}")

        finally:
            conn.close()


    reset_database()

    thread1 = Thread(target=transaction_1, args=("READ UNCOMMITTED",))
    thread2 = Thread(target=transaction_2, args=("READ UNCOMMITTED",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    print("________________________________________________________")

    reset_database()
    tx_1_update.clear()
    tx_2_read.clear()

    thread1 = Thread(target=transaction_1, args=("READ COMMITTED",))
    thread2 = Thread(target=transaction_2, args=("READ COMMITTED",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

def non_repeatable_read():
    tx_1_update = Event()
    tx_2_read = Event()

    def transaction_1(conn_str):
        conn = get_connection(conn_str)

        try:
            print(f"\n[Tx1-{conn_str}] Starting transaction 1.")
            conn.start_transaction()

            tx_2_read.wait()

            update_balance(conn, "Alice", -400, f"Tx1-{conn_str}")
            update_balance(conn, "Bob", 400, f"Tx1-{conn_str}")

            conn.commit()
            tx_1_update.set()


        except mysql.connector.Error as e:
            print(f"[ERROR] Error starting transaction 1: {e}")
            conn.rollback()
            print("Transaction 1 rollbacked")

        finally:
            conn.close()

    def transaction_2(conn_str):
        conn = get_connection(conn_str)

        try:
            print(f"\n[Tx2-{conn_str}] Starting transaction 2.")
            conn.start_transaction()

            alice_balance = fetch_balance(conn, "Alice")
            bob_balance = fetch_balance(conn, "Bob")

            print(f"\n[Tx2-{conn_str}] Sum balance (1): {alice_balance + bob_balance:.2f}")

            tx_2_read.set()

            tx_1_update.wait()

            bob_balance = fetch_balance(conn, "Bob")
            print(f"\n[Tx2-{conn_str}] Sum balance (2): {alice_balance + bob_balance:.2f}")

        except mysql.connector.Error as e:
            print(f"[ERROR] Error starting transaction 2: {e}")

        finally:
            conn.close()


    reset_database()

    thread1 = Thread(target=transaction_1, args=("READ COMMITTED",))
    thread2 = Thread(target=transaction_2, args=("READ COMMITTED",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    print("__________________________________________________________")

    reset_database()

    tx_1_update.clear()
    tx_2_read.clear()

    thread1 = Thread(target=transaction_1, args=("REPEATABLE READ",))
    thread2 = Thread(target=transaction_2, args=("REPEATABLE READ",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

def phantom_read():
    tx_1_insert = Event()
    tx_2_read = Event()

    def transaction_1(conn_str):
        conn = get_connection(conn_str)

        try:
            print(f"\n[Tx1-{conn_str}] Starting transaction 1.")
            conn.start_transaction()

            tx_2_read.wait()

            insert(conn, "Michael", 600, f"Tx1-{conn_str}")
            conn.commit()

            tx_1_insert.set()

        except mysql.connector.Error as e:
            print(f"[ERROR] Error starting transaction 1: {e}")
            conn.rollback()
            print("Transaction 1 rollbacked")

        finally:
            conn.close()

    def transaction_2(conn_str):
        conn = get_connection(conn_str)

        try:
            print(f"\n[Tx2-{conn_str}] Starting transaction 2.")
            conn.start_transaction()

            count = count_accounts(conn, f"Tx2-{conn_str}")
            print(f"\n[Tx2-{conn_str}] Count: {count}")
            tx_2_read.set()

            tx_1_insert.wait()

            sum = sum_balance(conn, f"Tx2-{conn_str}")

            if not sum:
                print("f\n[Tx2-{sum}] Error to get the sum")
                return

            print(f"\n[Tx2-{sum}] Count: {sum}")

            print(f"\nTx2-{conn_str} AVERAGE: {(sum / count):.2f}")


        except mysql.connector.Error as e:
            print(f"[ERROR] Error starting transaction 2: {e}")

        finally:
            conn.close()

    reset_database()

    thread1 = Thread(target=transaction_1, args=("READ COMMITTED",))
    thread2 = Thread(target=transaction_2, args=("READ COMMITTED",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()
    print("____________________________________________________________")

    reset_database()
    thread1 = Thread(target=transaction_1, args=("SERIALIZABLE",))
    thread2 = Thread(target=transaction_2, args=("SERIALIZABLE",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

def task_1():
    reset_database()
    dirty_read()

    reset_database()
    non_repeatable_read()

    reset_database()
    phantom_read()

# task_1()