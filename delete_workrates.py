import sqlite3

with sqlite3.connect('database.sqlite') as conn:
    cursor = conn.cursor()

    work_rates = ["attacking_work_rate", "defensive_work_rate"]
    work_rates_stats = ("low", "medium", "high")

    sql_ask = f"""
                        Select id, attacking_work_rate, defensive_work_rate
                        from Player_Attributes where
                        {work_rates[0]} NOT IN {work_rates_stats} OR
                        {work_rates[1]} NOT IN {work_rates_stats}
                        """
    cursor.execute(sql_ask)
    missing = cursor.fetchall()

    sql_ask = "Select id from Player_Attributes"
    cursor.execute(sql_ask)
    count = cursor.fetchall()

    print(missing)

    print(f"{(len(missing)/len(count))*100:.2f}%")

    sql_ask = f"""
                DELETE from Player_Attributes where 
                {work_rates[0]} NOT IN {work_rates_stats} OR
                {work_rates[1]} NOT IN {work_rates_stats}
                """

    cursor.execute(sql_ask)

    sql_ask = f"""
                            Select id, attacking_work_rate, defensive_work_rate
                            from Player_Attributes where
                            {work_rates[0]} NOT IN {work_rates_stats} OR
                            {work_rates[1]} NOT IN {work_rates_stats}
                            """
    cursor.execute(sql_ask)
    missing = cursor.fetchall()

    sql_ask = "Select id from Player_Attributes"
    cursor.execute(sql_ask)
    count = cursor.fetchall()

    print(f"{(len(missing) / len(count)) * 100:.2f}%")