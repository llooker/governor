import configparser
import looker_sdk
import sqlite3
import pandas as pd
import datetime
CONFIG_FILE = 'credentials.ini'
TABLE = 'usage_vw'
DB_FILENAME = "usage.db"

config = configparser.ConfigParser()
config.read(CONFIG_FILE)


looker_query = {
	"dynamic_fields": "[{\"category\":\"dimension\",\"expression\":\"if(is_null(${query.model}),${history.source},${query.model})\",\"label\":\"Model\",\"value_format\":null,\"value_format_name\":null,\"dimension\":\"model\",\"_kind_hint\":\"dimension\",\"_type_hint\":\"string\"}]",
	"fields": [
	  "user.email", 
	  "history.created_date", 
	  "history.query_run_count", 
	  "model"
	],
	"fill_fields": [],
	"filters": {
		"user.email": "-NULL",
		"history.created_date": "120 days"
	},
	"filter_expression": None,
	"limit": 100000,
	"model": "system__activity",
	"view": "history",
}


def check_table(cursor: sqlite3.Cursor):
    tbl_exist = cursor.execute(f"""
        SELECT 
            tbl_name 
        FROM 
            sqlite_master 
        WHERE 
            tbl_name='{TABLE}'; 
        """).fetchall()
    if not tbl_exist:
        cursor.execute(f"""
        create VIEW {TABLE} as 
        WITH latest as (
        SELECT
            "User Email",
            "History Created Date",
            "Instance",
            "Model",
            max("Insert Timestamp") as latest
        FROM 
            usage_dump
        GROUP BY
            1,2,3,4
        HAVING COUNT(*) > 1
        )
        SELECT 
            t.* 
        FROM
            usage_dump t
            LEFT JOIN latest ON (
                    t."User Email" = latest."User Email" AND
                    t."History Created Date" = latest."History Created Date" AND
                    t."Instance" = latest."Instance" AND
                    t."Model" = latest."Model" AND 
                    t."Insert Timestamp" < latest.latest
            )
        WHERE
        latest."User Email" IS NULL
            """)

if __name__ == '__main__':
    connection = sqlite3.connect(DB_FILENAME)
    cursor = connection.cursor()


    for section in config.sections():

        looker = looker_sdk.init40(config_file=CONFIG_FILE,section=section)

        tmp_result = looker.run_inline_query(result_format='csv',body=looker_query)

        f = open(f'tmp/{section}.csv','w')
        f.write(tmp_result)
        f.close()
        u = pd.read_csv(f'tmp/{section}.csv')
        u['Instance'] = section
        u['Insert Timestamp'] = datetime.datetime.now()
        u.to_sql('usage_dump',connection,if_exists='append')

    connection.commit()
    cursor.close()

    