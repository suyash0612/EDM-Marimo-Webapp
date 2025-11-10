import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _():
    return


@app.cell
def _():

    import sqlalchemy
    import urllib.parse
    from sqlalchemy import create_engine
    from sqlalchemy.sql import text

    # server='badm-server-1.database.windows.net'
    # user='badm-server-admin-1'
    # password='Suyash@7610!'
    # database='badm-test-1'

    server='badm-554.database.windows.net'
    user='Group-14'
    password='Abhinav@1'
    database='EDM_Group_14'


    # Define connection parameters
    driver = "ODBC Driver 17 for SQL Server"  # Ensure this driver is installed

    # Build the ODBC connection string
    odbc_str = f"DRIVER={{{driver}}};SERVER={server};PORT=1433;DATABASE={database};UID={user};PWD={password};LoginTimeout=30" # Increased timeout

    # Encode the connection string for SQLAlchemy
    connection_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"

    # Create SQLAlchemy engine
    engine = create_engine(connection_url)

    with engine.connect() as connection:
        result = connection.execute(text("SELECT GETDATE();"))
        for row in result:
            print("Connected! Server date and time:", row[0])
    return engine, text


@app.cell
def _(engine, text):
    def run_query(query):
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            for row in rows:
                print(row)

    query = """SELECT city, state, AVG(stars) AS avg_rating, COUNT(*) AS total_restaurants
    FROM business_table
    WHERE categories LIKE '%Restaurant%'
    GROUP BY city, state
    HAVING COUNT(*) > 100
    ORDER BY avg_rating DESC, total_restaurants DESC;"""

    run_query(query)
    return


@app.cell
def _(engine, text):
    import pandas as pd
    import matplotlib.pyplot as plt

    # Run the query and store results in a DataFrame
    def get_restaurant_data():
        query = """
            SELECT city, state, AVG(stars) AS avg_rating, COUNT(*) AS total_restaurants
            FROM business_table
            WHERE categories LIKE '%Restaurant%'
            GROUP BY city, state
            HAVING COUNT(*) > 100
            ORDER BY avg_rating DESC, total_restaurants DESC;
        """
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    # Fetch data
    df = get_restaurant_data()

    # Display top few rows
    print(df.head())

    # Create a bar chart for top 10 cities by average rating
    top10 = df.head(10)

    plt.figure(figsize=(10,6))
    plt.barh(top10["city"] + ", " + top10["state"], top10["avg_rating"], color="skyblue")
    plt.gca().invert_yaxis()  # highest rating on top
    plt.title("Top 10 Cities by Average Restaurant Rating")
    plt.xlabel("Average Rating")
    plt.ylabel("City, State")
    plt.show()

    return (top10,)


@app.cell
def _(engine):
    def _():
        import pandas as pd
        import plotly.express as px
        from sqlalchemy.sql import text

        def get_restaurant_data():
            query = """
                SELECT city, state, AVG(stars) AS avg_rating, COUNT(*) AS total_restaurants
                FROM business_table
                WHERE categories LIKE '%Restaurant%'
                GROUP BY city, state
                HAVING COUNT(*) > 100
                ORDER BY avg_rating DESC, total_restaurants DESC;
            """
            with engine.connect() as conn:
                result = conn.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df

        # Fetch data
        df = get_restaurant_data()

        # Display preview
        print(df.head())

        # ---- Interactive bar chart ----
        top10 = df.head(10)

        fig = px.bar(
            top10,
            x="avg_rating",
            y=top10["city"] + ", " + top10["state"],
            orientation="h",
            color="avg_rating",
            color_continuous_scale="Blues",
            title="Top 10 Cities by Average Restaurant Rating",
            labels={"avg_rating": "Average Rating", "y": "City, State"}
        )

        fig.update_layout(
            yaxis=dict(autorange="reversed"),
            height=500,
            xaxis_title="Average Rating",
            yaxis_title="City, State",
            template="plotly_white"
        )
        return fig.show()


    _()
    return


@app.cell
def _(top10):
    import plotly.graph_objects as go

    fig = go.Figure()

    # Add both charts as traces
    fig.add_trace(go.Bar(
        x=top10["avg_rating"], 
        y=top10["city"] + ", " + top10["state"], 
        orientation="h", 
        name="Average Rating",
        marker_color="skyblue"
    ))

    fig.add_trace(go.Bar(
        x=top10["total_restaurants"], 
        y=top10["city"] + ", " + top10["state"], 
        orientation="h", 
        name="Total Restaurants",
        marker_color="orange",
        visible=False
    ))

    # Dropdown menu
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=[
                    dict(label="Average Rating",
                         method="update",
                         args=[{"visible": [True, False]},
                               {"title": "Top 10 Cities by Average Rating"}]),
                    dict(label="Total Restaurants",
                         method="update",
                         args=[{"visible": [False, True]},
                               {"title": "Top 10 Cities by Total Restaurants"}])
                ],
                direction="down",
                showactive=True
            )
        ],
        yaxis=dict(autorange="reversed"),
        template="plotly_white",
        height=500
    )

    fig.show()

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
