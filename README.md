# Ingestion-workflow
n8n/
For ingestion, I will use n8n and DuckDB. DuckDB is an in-memory analytical processing database (OLAP) that is perfect for this type of quick analysis task, as it does not require a separate server and can be used directly with files.
- For data ingestion 
- in-memory analytical processing database (OLAP). 
Node 'HTTP Request': Downloads the ads_spend.csv file from the Google Drive link.  
Node 'Read Binary File': Reads the downloaded CSV file.  
Node 'CSV to JSON': Converts the CSV data into JSON format.  
Node 'DuckDB': This is the key node. Connects to a DuckDB database.  
 Action: Execute SQL. 
SQL/
CREATE OR REPLACE TABLE ads_spend_raw (
            date DATE,
            platform VARCHAR,
            account VARCHAR,
            campaign VARCHAR,
            country VARCHAR,
            device VARCHAR,
            spend FLOAT,
            clicks INT,
            impresions INT,
            conversions INT,
            load_date TIMESTAMP,
            source_file VARCHAR
        );

        INSERT INTO ads_spend_raw
        (date, platform, account, campaign, country, device, spend, clicks, impressions, conversions, load_date, source_file)
        SELECT
          date::DATE,
          platform::VARCHAR,
          account::VARCHAR,
          campaign::VARCHAR,
          country::VARCHAR,
          device::VARCHAR,
          spend::FLOAT,
          clicks::INT,
          impresions::INT,
          conversions::INT,
          NOW() AS load_date,
          'ads_spend.csv' AS source_file
        FROM read_csv_auto('{{ $json.data }}', { 'delimiter': ',' });
  insert into: inser data from file CSV
  NOW(): capture date and hour from metadata
  'ads_spend.csv': original file name
SQL model
To calculate CAC and ROAS and compare them, we'll use a SQL query that can be run in DuckDB. The key is to calculate the metrics for both time periods and then combine them to show the delta.
WITH
      metrics_last_30_days AS (
        SELECT
          SUM(spend) AS total_spend,
          SUM(conversions) AS total_conversions
        FROM ads_spend_raw
        WHERE
          date >= '{max_date}'::DATE - INTERVAL '29 days'
          AND date <= '{max_date}'::DATE
      ),
      metrics_prior_30_days AS (
        SELECT
          SUM(spend) AS total_spend,
          SUM(conversions) AS total_conversions
        FROM ads_spend_raw
        WHERE
          date >= '{max_date}'::DATE - INTERVAL '59 days'
          AND date < '{max_date}'::DATE - INTERVAL '29 days'
      ),
      combined_metrics AS (
        SELECT
          t1.total_spend AS current_spend,
          t1.total_conversions AS current_conversions,
          t2.total_spend AS previous_spend,
          t2.total_conversions AS previous_conversions
        FROM metrics_last_30_days t1
        JOIN metrics_prior_30_days t2
          ON 1 = 1
      )
    SELECT
      CAST(current_spend AS DECIMAL(10, 2)) AS current_spend,
      CAST(current_conversions AS INT) AS current_conversions,
      CAST(
        CASE
          WHEN current_conversions > 0 THEN current_spend / current_conversions
          ELSE NULL
        END AS DECIMAL(10, 2)
      ) AS CAC_current,
      CAST(
        CASE
          WHEN current_spend > 0 THEN (current_conversions * 100) / current_spend
          ELSE NULL
        END AS DECIMAL(10, 2)
      ) AS ROAS_current,
      CAST(
        (
          (
            (current_spend / current_conversions) - (previous_spend / previous_conversions)
          ) / (previous_spend / previous_conversions)
        ) * 100 AS DECIMAL(10, 2)
      ) AS delta_CAC_percentage,
      CAST(
        (
          (
            ((current_conversions * 100) / current_spend) - ((previous_conversions * 100) / previous_spend)
          ) / ((previous_conversions * 100) / previous_spend)
        ) * 100 AS DECIMAL(10, 2)
      ) AS delta_ROAS_percentage
    FROM combined_metrics;
explanation 
1.•	CTE (Common Table Expressions):
metrics_last_30_days and metrics_prior_30_days calculate spending and conversions for each respective period. This improves query readability. 
2.•	KPI Calculations:
The final query computes CAC (spend/conversions) and ROAS ((conversions * 100)/spend) for the current period. 
3.•	Delta:
The delta is calculated as (Current_Value − Previous_Value) / Previous_Value * 100 to show the percentage change. CASE statements handle division-by-zero cases. 
•	Table Format:
The output is a compact table displaying absolute values and percentage deltas.
Analist access/
the choice is make a script SQL parameterizable
SQL model
WITH
      metrics_current_period AS (
        SELECT
          'current_period' AS period,
          SUM(spend) AS spend,
          SUM(conversions) AS conversions
        FROM ads_spend_raw
        WHERE
          date BETWEEN '{start_date}' AND '{end_date}'
      ),
      metrics_prior_period AS (
        SELECT
          'prior_period' AS period,
          SUM(spend) AS spend,
          SUM(conversions) AS conversions
        FROM ads_spend_raw
        WHERE
          date BETWEEN '{prior_start_date}' AND '{prior_end_date}'
      )
    SELECT
      period,
      CAST(spend AS DECIMAL(10, 2)) AS spend,
      CAST(conversions AS INT) AS conversions,
      CAST((spend / conversions) AS DECIMAL(10, 2)) AS CAC,
      CAST(((conversions * 100) / spend) AS DECIMAL(10, 2)) AS ROAS
    FROM metrics_current_period
    UNION ALL
    SELECT
      period,
      CAST(spend AS DECIMAL(10, 2)) AS spend,
      CAST(conversions AS INT) AS conversions,
      CAST((spend / conversions) AS DECIMAL(10, 2)) AS CAC,
      CAST(((conversions * 100) / spend) AS DECIMAL(10, 2)) AS ROAS
    FROM metrics_prior_period;
  Explanation
  Parameters: Placeholders :start_date and :end_date are used so the analyst can easily define the date range.
Previous Period Logic: The second CTE calculates the previous period based on the duration of the current period. For example, if the current period is January 15-30, the previous one will be January 1-14.
Results Union: UNION ALL combines the results of both periods into a single table, simplifying comparison.  
the final step is for the agent’s part, the objective is to show how a natural language question maps to an SQL query. The idea is for the agent to interpret the user’s intent (“compare CAC and ROAS”) and the time parameters (“last 30 days vs. the previous 30 days”) to construct the correct query. 
Agent Process: 
1.	Identify the Intent: The agent detects that the intent is to “compare metrics.” The metrics are CAC and ROAS. 
2.	Extract Time Parameters: The agent identifies two time periods: 
o	Period 1: “Last 30 days” → Translates to a date range: current_date - 30 days to current_date. 
o	Period 2: “Previous 30 days” → Translates to a date range: current_date - 60 days to current_date - 31 days.
3.	Select SQL Template: The agent uses a predefined template for metric comparison, similar to the one shown in Part 2. 
4.	Generate the SQL Query: The agent inserts the date ranges and metrics into the SQL template to produce the final query.
Results/
• Part 1 (Ingestion): The script downloads the file, processes it, and successfully loads it into a DuckDB table. The line “ Data ingestion successful in DuckDB”
• Part 2 (Modeling): The SQL query for KPI delta_CAC_percentage: -7.63 and delta_ROAS_percentage: 8.26
• Part 3 (Analyst Access): ): The parameterized query with date ranges (2025-01-16 to 2025-01-30) the results show that spend, conversions, CAC, and ROAS were calculated correctly for both periods.
<img width="808" height="363" alt="imagen" src="https://github.com/user-attachments/assets/99ab9e30-7039-40b7-8b8c-70956b32fad0" />
