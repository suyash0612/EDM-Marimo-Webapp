# db_viz_app.py
# Marimo Restaurant Data Visualization App for Tampa Market Analysis
# Inspired by Milestone 3: Extract Business Insights

import marimo as mo
from sqlalchemy import create_engine
app = mo.App(width="full")

@app.cell
def _():
    # keep imports local to a cell for Marimo's dependency tracking
    import os
    os.environ["POLARS_NO_ARROW"] = "1"  # we don't use polars, but prevents stray arrow imports

    import urllib.parse
    import pandas as pd
    import time
    import altair as alt
    import marimo as mo
    from sqlalchemy import create_engine, text
    
    return os, urllib, pd, time, alt, mo, create_engine, text

@app.cell
def _(os):
    # Database credentials from environment variables
    # To set environment variables in terminal before running:
    # export SQL_SERVER="badm-554.database.windows.net"
    # export SQL_USER="Group-14"
    # export SQL_PASSWORD="Abhinav@1"
    # export SQL_DATABASE="EDM_Group_14"
    # export SQL_DRIVER="ODBC Driver 17 for SQL Server"
    
    SQL_SERVER   = os.getenv("SQL_SERVER",   "badm-554.database.windows.net")
    SQL_USER     = os.getenv("SQL_USER",     "Group-14")
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "Abhinav@1")  # Update with your actual password
    SQL_DATABASE = os.getenv("SQL_DATABASE", "EDM_Group_14")
    SQL_DRIVER   = os.getenv("SQL_DRIVER",   "ODBC Driver 17 for SQL Server")
    
    # Debug: Show connection info (without password)
    connection_info = f"Server: {SQL_SERVER}, User: {SQL_USER}, Database: {SQL_DATABASE}"
    
    return SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_DATABASE, SQL_DRIVER, connection_info

@app.cell
def _(SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_DATABASE, SQL_DRIVER, urllib, create_engine, mo, text):
    # Build ODBC connection string WITH improved timeout and connection settings
    driver_with_braces = "{" + SQL_DRIVER + "}"

    odbc_kv = [
        f"DRIVER={driver_with_braces}",
        f"SERVER={SQL_SERVER}",
        "PORT=1433",
        f"DATABASE={SQL_DATABASE}",
        f"UID={SQL_USER}",
        f"PWD={SQL_PASSWORD}",
        "Encrypt=yes",
        "TrustServerCertificate=no",
        "Connection Timeout=60",
        "LoginTimeout=60",
    ]
    odbc_str = ";".join(odbc_kv)

    connection_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_str)

    # Create engine with improved connection pooling and recycling
    engine = create_engine(
        connection_url,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=5,
        max_overflow=10,
        connect_args={"timeout": 60}
    )
    
    # Test connection
    connection_status = "❌ Not tested yet"
    try:
        with engine.connect() as _conn:
            test_result = _conn.execute(text("SELECT 1"))
            connection_status = "Connected successfully to Azure SQL ✅ "
    except Exception as e:
        connection_status = f"Connection failed to Azure SQL: {str(e)[:150]} ❌ "
    
    return engine, connection_status


@app.cell
def _(mo):
    show_instructions = mo.ui.switch(value=False, label="*Instructions To Use Dashboard (Recommended)*📚")
    return show_instructions

@app.cell
def _(mo, show_instructions, connection_status):
    instructions = mo.md("""
    ## 📖 Quick Start Guide
    
    **1️⃣ Select Query** → Choose from 10 pre-written queries or create a custom SQL query to Azure SQL Database
    
    **2️⃣ Execute** → Data automatically fetches from Azure SQL Database
    
    **3️⃣ Visualize** → Pick chart type (Bar/Scatter/Line/Histogram) and select X/Y axes
    
    **4️⃣ Analyze** → Review insights and patterns using charts
    
    ### Chart Types:
    - **Bar Chart**: Compare values across categories
    - **Scatter Plot**: Find relationships between variables
    - **Line Chart**: Track trends over time
    - **Histogram**: Show distribution patterns
    
    ### Example Workflows:
    - *Market Gaps*: Use "Market Saturation" → Bar Chart (rating_bracket vs count)
    - *Trends*: Use "Sentiment Over Time" → Line Chart (month vs avg_rating)
    - *Top Performers*: Use "Performance" → Scatter Plot (review_count vs stars, color by status)
    - *Cuisines*: Use "Top Cuisines" → Bar Chart (cuisine vs count)

    **Note that some queries may take longer to execute depending on the data size**
    """)
    
    header = mo.vstack([
        mo.md("###EDM Project 1 | Author: Suyash Sawant | Email: ssawant4@illinois.edu"),
        mo.md("#"),
        mo.md("# 🍔 Tampa Restaurants Market Analysis Dashboard"),
        mo.md("*Interactive Business Intelligence Platform - Explore trends, relationships & opportunities*"),
        mo.callout(connection_status, kind="success" if "successfully" in connection_status else "warn"),
        show_instructions,
    ])
    
    if show_instructions.value:
        header = mo.vstack([header, instructions])
    
    header

@app.cell
def _(mo):
    predefined_queries_dict = {
        "Average Rating by Star Count": """
            SELECT stars, COUNT(*) AS restaurant_count, AVG(review_count) AS avg_reviews
            FROM business_table
            WHERE city = 'Tampa' AND categories LIKE '%Restaurant%'
            GROUP BY stars
            ORDER BY stars;
        """,
        "Top 15 Cuisines by Restaurant Count": """
            SELECT TOP 15
                TRIM(value) AS cuisine,
                COUNT(DISTINCT business_id) AS restaurant_count
            FROM business_table
            CROSS APPLY STRING_SPLIT(categories, ',')
            WHERE city = 'Tampa' AND TRIM(value) NOT IN ('Restaurants', 'Food')
            GROUP BY TRIM(value)
            ORDER BY restaurant_count DESC;
        """,
        "Restaurant Performance: Ratings vs Reviews": """
            SELECT TOP 50
                name,
                stars,
                review_count,
                is_open
            FROM business_table
            WHERE city = 'Tampa' AND categories LIKE '%Restaurant%' AND review_count > 50
            ORDER BY review_count DESC;
        """,
        "Market Saturation: Reviews by Rating Bracket": """
            SELECT 
                CASE 
                    WHEN stars >= 4.5 THEN '4.5+ Stars (Excellent)'
                    WHEN stars >= 4 THEN '4-4.5 Stars (Very Good)'
                    WHEN stars >= 3.5 THEN '3.5-4 Stars (Good)'
                    WHEN stars >= 3 THEN '3-3.5 Stars (Average)'
                    ELSE 'Below 3 Stars (Poor)'
                END AS rating_bracket,
                COUNT(*) AS restaurant_count,
                AVG(review_count) AS avg_review_count,
                AVG(CAST(is_open AS INT)) AS pct_open
            FROM business_table
            WHERE city = 'Tampa' AND categories LIKE '%Restaurant%'
            GROUP BY 
                CASE 
                    WHEN stars >= 4.5 THEN '4.5+ Stars (Excellent)'
                    WHEN stars >= 4 THEN '4-4.5 Stars (Very Good)'
                    WHEN stars >= 3.5 THEN '3.5-4 Stars (Good)'
                    WHEN stars >= 3 THEN '3-3.5 Stars (Average)'
                    ELSE 'Below 3 Stars (Poor)'
                END
            ORDER BY rating_bracket DESC;
        """,
        "Review Activity Trends": """
            SELECT TOP 12
                YEAR(date) AS review_year,
                MONTH(date) AS review_month,
                COUNT(*) AS review_count
            FROM review_table
            WHERE business_id IN (
                SELECT business_id FROM business_table 
                WHERE city = 'Tampa' AND categories LIKE '%Restaurant%'
            )
            GROUP BY YEAR(date), MONTH(date)
            ORDER BY review_year DESC, review_month DESC;
        """,
        "Top 20 Most Reviewed Restaurants": """
            SELECT TOP 20
                name,
                stars,
                review_count,
                CASE WHEN is_open = 1 THEN 'Open' ELSE 'Closed' END AS status
            FROM business_table
            WHERE city = 'Tampa' AND categories LIKE '%Restaurant%'
            ORDER BY review_count DESC;
        """,
        "Average Review Sentiment Over Time": """
            SELECT TOP 12
                YEAR(r.date) AS year,
                MONTH(r.date) AS month,
                AVG(r.stars) AS avg_rating,
                COUNT(*) AS total_reviews
            FROM review_table r
            WHERE r.business_id IN (
                SELECT business_id FROM business_table 
                WHERE city = 'Tampa' AND categories LIKE '%Restaurant%'
            )
            GROUP BY YEAR(r.date), MONTH(r.date)
            ORDER BY year DESC, month DESC;
        """,
        "Active vs Inactive Restaurants": """
            SELECT 
                CASE WHEN is_open = 1 THEN 'Active' ELSE 'Inactive' END AS status,
                COUNT(*) AS count,
                AVG(stars) AS avg_rating,
                AVG(review_count) AS avg_reviews
            FROM business_table
            WHERE city = 'Tampa' AND categories LIKE '%Restaurant%'
            GROUP BY is_open;
        """,
        "Top 10 Highest Rated Restaurants": """
            SELECT TOP 10
                name,
                stars,
                review_count,
                categories
            FROM business_table
            WHERE city = 'Tampa' AND categories LIKE '%Restaurant%' AND stars >= 4
            ORDER BY stars DESC, review_count DESC;
        """,
        "Custom Query": "",
    }
    
    query_selector = mo.ui.dropdown(
        options=list(predefined_queries_dict.keys()),
        value="Average Rating by Star Count",
        label="📊 Select Analysis"
    )
    query_selector, predefined_queries_dict

@app.cell
def _(mo, query_selector, predefined_queries_dict):
    selected_query = predefined_queries_dict.get(query_selector.value, "")
    
    query_text = mo.ui.text_area(
        label="SQL Query",
        value=selected_query,
        full_width=True,
    )
    query_text

@app.cell
def _(engine, text, query_text, pd, time):
    # Execute query with retry logic and better error handling
    sql = query_text.value.strip()
    records, columns, error, df = [], [], None, None
    
    if not sql:
        error = "Query is empty."
    else:
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                with engine.connect() as _query_conn:
                    query_result = _query_conn.execute(text(sql))
                    columns = list(query_result.keys())
                    records = [dict(row._mapping) for row in query_result.fetchall()]
                    df = pd.DataFrame(records) if records else pd.DataFrame()
                break  # Success, exit retry loop
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    error = f"{type(e).__name__}: {str(e)[:200]}"
                else:
                    time.sleep(2)  # Wait 2 seconds before retry
    
    records, columns, error, df

@app.cell
def _(columns, mo):
    # Chart type and axis selection
    chart_type = None
    x_axis = None
    y_axis = None
    color = None
    show_table = mo.ui.switch(value=False, label="📋 Show Data Table")
    
    if not columns:
        mo.md("**⏳ Run a query to see visualization options**")
    else:
        mo.md("### 🎨 Visualization Controls")
        chart_type = mo.ui.dropdown(
            ["Bar Chart", "Scatter Plot", "Line Chart", "Histogram"],
            value="Bar Chart",
            label="📈 Chart Type"
        )
        x_axis = mo.ui.dropdown(columns, value=columns[0], label="📌 X-Axis")
        
        # pick a likely numeric target
        preferred = ("count", "value", "amount", "score", "avg", "avg_rating", "avg_reviews", "total", "sum", "stars", "restaurant_count", "review_count")
        default_y = next((c for c in columns if c.lower() in preferred), columns[-1] if len(columns) > 1 else columns[0])
        y_axis = mo.ui.dropdown(columns, value=default_y, label="📊 Y-Axis")
        color = mo.ui.dropdown(["None"] + columns, value="None", label="🎨 Color (optional)")
        
        mo.vstack([chart_type, x_axis, y_axis, color, show_table])
    
    chart_type, x_axis, y_axis, color, show_table

@app.cell
def _(alt, pd):
    # Enhanced chart builder with intelligent column type detection
    def build_chart(records, columns, chart_type, x_field, y_field, color_field):
        if not columns or not records:
            return alt.Chart(alt.Data(values=[])).mark_text().encode(
                text=alt.value("No data available")
            ).properties(width=700, height=420, title="No Data")

        if x_field not in columns or y_field not in columns:
            return alt.Chart(alt.Data(values=[])).mark_text().encode(
                text=alt.value("Invalid axis selection")
            ).properties(width=700, height=420, title="Invalid Selection")

        # Convert to pandas DataFrame for Altair to properly infer types
        data = pd.DataFrame(records)
        
        # Smart type detection for fields
        def get_field_type(field_name):
            if any(keyword in field_name.lower() for keyword in ['count', 'total', 'sum', 'avg', 'rating', 'stars', 'review']):
                return ":Q"  # Quantitative
            elif any(keyword in field_name.lower() for keyword in ['date', 'time', 'year', 'month']):
                return ":T"  # Temporal
            else:
                return ":N"  # Nominal
        
        x_type = get_field_type(x_field)
        y_type = get_field_type(y_field)
        color_type = get_field_type(color_field) if color_field != "None" else None
        
        # Build color encoding with explicit type
        if color_field == "None":
            color_enc = alt.value("steelblue")
        else:
            color_enc = alt.Color(f"{color_field}{color_type}", legend=alt.Legend(title=color_field))

        if chart_type == "Bar Chart":
            chart = alt.Chart(data).mark_bar().encode(
                x=alt.X(x_field + x_type, title=x_field),
                y=alt.Y(y_field + y_type, title=y_field),
                color=color_enc,
                tooltip=[x_field, y_field, color_field] if color_field != "None" else [x_field, y_field],
            ).properties(width=700, height=420, title=f"{y_field} by {x_field}")
        
        elif chart_type == "Scatter Plot":
            chart = alt.Chart(data).mark_circle(size=100).encode(
                x=alt.X(x_field + x_type, title=x_field),
                y=alt.Y(y_field + y_type, title=y_field),
                color=color_enc,
                tooltip=[x_field, y_field, color_field] if color_field != "None" else [x_field, y_field],
            ).properties(width=700, height=420, title=f"{y_field} vs {x_field}")
        
        elif chart_type == "Line Chart":
            chart = alt.Chart(data).mark_line(point=True).encode(
                x=alt.X(x_field + x_type, title=x_field),
                y=alt.Y(y_field + y_type, title=y_field),
                color=color_enc,
                tooltip=[x_field, y_field, color_field] if color_field != "None" else [x_field, y_field],
            ).properties(width=700, height=420, title=f"{y_field} by {x_field}")
        
        elif chart_type == "Histogram":
            chart = alt.Chart(data).mark_bar().encode(
                x=alt.X(x_field + ":Q", bin=alt.Bin(maxbins=30), title=x_field),
                y=alt.Y("count()", title="Count"),
                color=color_enc,
                tooltip=["count()"],
            ).properties(width=700, height=420, title=f"Distribution of {x_field}")
        
        else:
            chart = alt.Chart(data).mark_bar().encode(
                x=alt.X(x_field + x_type, title=x_field),
                y=alt.Y(y_field + y_type, title=y_field),
                color=color_enc,
            ).properties(width=700, height=420, title="Chart")

        return chart
    
    build_chart

@app.cell
def _(records, columns, error, chart_type, x_axis, y_axis, color, build_chart, df, show_table, mo):
    # Render visualization-focused dashboard
    ui = []
    
    # Display errors if any
    if error:
        ui.append(mo.callout(f"⚠️ **Query Error:** {error}", kind="warn"))
    elif not records:
        ui.append(mo.callout("📌 Select an analysis to visualize data", kind="info"))
    else:
        # Display query results count
        ui.append(mo.md(f"### 📈 Visualization Results ({len(records)} rows)"))
        
        # Display chart as main focus
        if records and columns and chart_type is not None and x_axis is not None and y_axis is not None:
            try:
                chart = build_chart(
                    records, columns,
                    chart_type.value, x_axis.value, y_axis.value, color.value
                )
                ui.append(mo.ui.altair_chart(chart))
            except Exception as e:
                ui.append(mo.callout(f"⚠️ **Chart Error:** {str(e)}", kind="warn"))
        
        # Display data table only if explicitly toggled on
        if show_table is not None and show_table.value and df is not None and not df.empty:
            ui.append(mo.md("### 📋 Detailed Data"))
            ui.append(mo.ui.dataframe(df.head(100)))
        
        # Dynamic insights based on data
        ui.append(mo.md("### 💡 Key Insights"))
        insights = []
        
        if 'restaurant_count' in columns or 'count' in columns:
            insights.append("📊 **Market Volume:** " + str(sum([r.get('restaurant_count', r.get('count', 0)) for r in records if isinstance(r, dict)])) + " restaurants analyzed")
        
        if 'avg_rating' in columns or 'avg_stars' in columns:
            ratings = [r.get('avg_rating', r.get('avg_stars')) for r in records if r.get('avg_rating') or r.get('avg_stars')]
            if ratings:
                insights.append(f"⭐ **Average Quality:** {sum(ratings)/len(ratings):.2f}/5 stars")
        
        if 'stars' in columns:
            insights.append("📈 **Rating Distribution:** Shows customer satisfaction levels across restaurants")
        
        if 'cuisine' in columns or 'category' in columns:
            insights.append("🍽️ **Category Leaders:** Top cuisine types in the market")
        
        if 'year' in columns or 'month' in columns:
            insights.append("📅 **Temporal Trends:** Shows how the market has evolved over time")
        
        if 'status' in columns or 'is_open' in columns:
            insights.append("🏪 **Business Status:** Active vs inactive establishments")
        
        if not insights:
            insights.append("💾 **Data Loaded:** Ready for analysis and exploration")
        
        ui.append(mo.md("\n".join(["- " + insight for insight in insights])))
    
    mo.vstack(ui)
