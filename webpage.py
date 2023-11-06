import pandas as pd
import streamlit as st

from db import DB


@st.cache_resource
def db_connection():
    return DB


# with db_connection() as db:
#     c = db.cursor()
#     c.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     tables = c.fetchall()
#     print(tables)
#     st.write(tables)
#     df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", db)
#     st.write(df)

HEADER = st.container()
OCD, OAS, OAM, OAP, GO = st.tabs(['OCD', 'OAS', 'OAM', 'OAP', 'GO'])


def default_sql_commands():
    return {
        'OCD': {
            'ocd_article.csv': "SELECT * FROM [ocd_article.csv];",
            'ocd_propertyclass.csv': "SELECT * FROM [ocd_propertyclass.csv];",
            'ocd_artbase.csv': "SELECT * FROM [ocd_artbase.csv];",
            'ocd_articletaxes.csv': "SELECT * FROM [ocd_articletaxes.csv];",
            # 'ocd_artshorttext.csv': "SELECT * FROM [ocd_artshorttext.csv];",
            # 'ocd_artlongtext.csv': "SELECT * FROM [ocd_artlongtext.csv];",
        },
        'OAS': {
            'article.csv': "SELECT * FROM [article.csv]",
            'text.csv': "SELECT * FROM [text.csv]",
        },
        'OAM': {
            'oam_article2ofml.csv': "SELECT * FROM [oam_article2ofml.csv]",
            'oam_article2odbparams.csv': "SELECT * FROM [oam_article2odbparams.csv]",
        }
    }


if 'sql_commands' not in st.session_state:
    st.session_state['sql_commands'] = default_sql_commands()

with HEADER:
    query_article = st.text_input('Artikel').upper().replace('*', '%')

    if st.button('Filtern'):

        if not query_article:
            st.session_state['sql_commands'] = default_sql_commands()

with OCD:
    if query_article:
        st.session_state['sql_commands']['OCD'][
            'ocd_article.csv'] = f"SELECT * FROM [ocd_article.csv] WHERE article_nr LIKE '{query_article}';"

        st.session_state['sql_commands']['OCD'][
            'ocd_propertyclass.csv'] = f"SELECT * FROM [ocd_propertyclass.csv] WHERE article_nr LIKE '{query_article}';"

        st.session_state['sql_commands']['OCD'][
            'ocd_artbase.csv'] = f"SELECT * FROM [ocd_artbase.csv] WHERE article_nr LIKE '{query_article}';"

        st.session_state['sql_commands']['OCD'][
            'ocd_articletaxes.csv'] = f"SELECT * FROM [ocd_articletaxes.csv] WHERE article_nr LIKE '{query_article}';"

    for table_name, sql in st.session_state['sql_commands']['OCD'].items():
        df = pd.read_sql(sql, DB)
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        st.write(df)

with OAS:
    if query_article:
        st.session_state['sql_commands']['OAS'][
            'article.csv'] = f"SELECT * FROM [article.csv] WHERE name LIKE '{query_article}';"

        st.session_state['sql_commands']['OAS'][
            'text.csv'] = f"SELECT * FROM [text.csv] WHERE name LIKE '{query_article}';"

    for table_name, sql in st.session_state['sql_commands']['OAS'].items():
        df = pd.read_sql(sql, DB)
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)

with OAM:
    if query_article:
        st.session_state['sql_commands']['OAM'][
            'oam_article2ofml.csv'] = f"SELECT * FROM [oam_article2ofml.csv] WHERE article LIKE '{query_article}';"

        st.session_state['sql_commands']['OAM'][
            'oam_article2odbparams.csv'] = f"SELECT * FROM [oam_article2odbparams.csv] WHERE article LIKE '{query_article}';"

    for table_name, sql in st.session_state['sql_commands']['OAM'].items():
        df = pd.read_sql(sql, DB)
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)
