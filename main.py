from flask import Flask, render_template, request
import mysql.connector as connection
import pandas as pd
import os

app = Flask(__name__)

is_connected = False


@app.route('/login', methods=['GET', 'POST'])
def connecting_db():
    if request.method == 'POST':
        if request.form["id"] == "root":
            if request.form["pass"] == "pass":
                if request.form["operation"] == "Mysql":
                    my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
                    global is_connected
                    is_connected = True
                    data = connect_mysql(my_db)
                    return render_template("my_sql.html", data=data)
                else:
                    return render_template('login_issue.html', data="This database is currently unavailable")
            else:
                return render_template("login_issue.html", data="The password is wrong")
        else:
            return render_template("login_issue.html", data="The id is wrong")

    else:
        return render_template("login_issue.html", data="The request method is wrong")


def connect_mysql(db):
    if db.is_connected():
        query = "show databases"
        cursor = db.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        lst = []
        for i in range(len(data)):
            lst.append(data[i][0])
        return lst
    else:
        return "DB connection is failed"


@app.route('/', methods=['GET', 'POST'])  # To render Homepage
def home_page():
    return render_template('index.html')


@app.route('/mysql_page', methods=['GET', 'POST'])
def mysql_page():
    return render_template('my_sql.html')


@app.route('/creation', methods=['GET', 'POST'])
def create_table():
    print(is_connected)
    if is_connected:
        my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
        lst = connect_mysql(my_db)
        cursor = my_db.cursor()
        try:
            if request.form["database"] in lst:
                create_table_query = f"CREATE TABLE {request.form['name']} ({request.form['columns']});"
                print(create_table_query)
                cursor.execute(f"use {request.form['database']};")
                cursor.execute(create_table_query)
                return render_template('result.html', data=f"table is created in {request.form['database']}")
            else:
                create_database_query = f"CREATE DATABASE {request.form['database']};"
                create_table_query = f"CREATE TABLE {request.form['name']} ({request.form['columns']});"
                print(create_table_query)
                cursor.execute(create_database_query)
                cursor.execute(f"use {request.form['database']}")
                cursor.execute(create_table_query)
                return render_template('result.html',
                                       data=f"new data base is created and table creation in {request.form['database']} is successful")
        except Exception as e:
            return render_template("result.html", data="table is already exists")
    else:
        return render_template('result.html', data="database is not connected")


@app.route('/insertion', methods=['POST', 'GET'])
def inserting_one_value():
    if is_connected:
        my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
        cursor1 = my_db.cursor()
        query_2 = f"insert into {request.form['database']}.{request.form['tableName']} ({request.form['attributes']}) values({request.form['values']});"

        print(query_2)
        cursor1.execute(str(query_2))
        my_db.commit()
        return render_template("result.html", data="insertion operation is successful")
    else:
        return render_template("Database connection is failed")


@app.route('/update', methods=['GET', 'POST'])
def updating_mysql():
    if is_connected:
        my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
        cursor1 = my_db.cursor()
        query_2 = f"UPDATE {request.form['database']}.{request.form['tableName']} SET {request.form['set_value']} WHERE {request.form['condition']};"
        print(query_2)
        cursor1.execute(query_2)
        my_db.commit()
        return render_template("result.html", data="Update operation is successful")
    else:
        return render_template("Database connection is failed")


@app.route('/delete_mysql', methods=['GET', 'POST'])
def delete_mysql():
    if is_connected:
        my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
        cursor1 = my_db.cursor()
        query_2 = f"DELETE FROM {request.form['database']}.{request.form['tableName']} WHERE {request.form['condition']};"
        print(query_2)
        cursor1.execute(query_2)
        my_db.commit()
        return render_template("result.html", data="Deletion operation is successful")
    else:
        return render_template("Database connection is failed")


@app.route('/bulk_insertion_mysql', methods=["GET", "POST"])
def bulk_insertion_mysql():
    path = request.form["path"]
    table_name = request.form["tableName"]
    database = request.form["database"]
    if is_connected:
        my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
        cursor = my_db.cursor()
        df = pd.read_csv(path)
        lst = [i + " varchar(50)" for i in df.columns]
        c = tuple(lst)
        columns_for_creating = ", ".join(c)
        q = f"use {database}"
        cursor.execute(q)
        columns_in_insert = ",".join(df.columns)
        try:
            for i in range(len(df)):
                data = tuple(df.iloc[i])
                print(data)
                qu = f"insert into {table_name}({columns_in_insert}) values{data}"
                cursor.execute(qu)
            my_db.commit()
        except Exception as e:
            query = f"create table {table_name} ({columns_for_creating});"
            cursor.execute(query)
            for i in range(len(df)):
                data = tuple(df.iloc[i])
                print(data)
                qu = f"insert into {table_name}({columns_in_insert}) values{data}"
                cursor.execute(qu)
            my_db.commit()
        finally:
            return render_template("result.html", data="Bulk insertion operation is successfull")
            my_db.commit()
    else:
        return render_template("result.html", data="Database connection is failed")


@app.route('/download_mysql', methods=["GET", "POST"])
def download_file_mysql():
    path_to_save = request.form["path_to_save"]
    table_name = request.form["tableName"]
    database = request.form["database"]
    filename = request.form["filename"]
    if is_connected:
        my_db = connection.Connect(host="localhost", user="root", passwd="Puni@123", use_pure=True)
        cursor = my_db.cursor()
        query1 = f"use {database};"
        query2 = f"select * from {table_name};"
        cursor.execute(query1)
        cursor.execute(query2)
        data = cursor.fetchall()
        final_data = pd.DataFrame(data)
        full_path = path_to_save + "\\" + filename + ".csv"
        try:
            temp_file = filename+".csv"
            a = str(temp_file)
            if a in os.listdir(path_to_save):
                print("kfjkjkjk")
                return render_template("result.html", data="File is already exists")
            else:
                raise Exception
        except Exception as e:
            print("fjdkasfasklfd")
            final_data.to_csv(full_path)
            return render_template("result.html", data=f"File is successfully downloaded as {full_path}")
    else:
        return render_template("result.html", data="Database connection is failed")


if __name__ == '__main__':
    app.run(debug=True)
