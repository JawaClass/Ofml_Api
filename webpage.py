import threading

import pandas as pd
import streamlit as st
from db import DB

# from websockets.sync.client import connect
#
# def hello():
#     with connect("ws://localhost:8765") as websocket:
#         websocket.send("Hello world!")
#         message = websocket.recv()
#         print(f"Received: {message}")

# hello()
from ws_client import ws_listen_as_client


def ws_on_open(*args, **kwargs):
    print('WEB :: ws_on_open', args, kwargs)


def ws_on_close(*args, **kwargs):
    print('WEB :: ws_on_close', args, kwargs)


def ws_on_message(*args, **kwargs):
    print('WEB :: ws_on_message', args, kwargs)


ws_client_thread = threading.Thread(target=ws_listen_as_client,
                                    args=(ws_on_open,
                                          ws_on_message,
                                          ws_on_close))

ws_client_thread.start()

# extend the streamlit browser container window default is 48rem
css = '''
<style>
    #root > div > div > div > div > div > section > div {
    max-width: 108rem;
    }

</style>
'''
st.markdown(css, unsafe_allow_html=True)


@st.cache_resource
def db_connection():
    return DB


with st.sidebar:
    sql_all_programs = "SELECT DISTINCT __sql__program__ FROM [ocd_article.csv];"
    with db_connection() as db:
        crx = db.cursor()
        crx.execute(sql_all_programs)
        all_programs = [_[0] for _ in crx.fetchall()]
        for program_name in all_programs:
            st.markdown(program_name)
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
        },
        'OAP': {
            'oap_metatype2type.csv': "SELECT * FROM [oap_metatype2type.csv]",
            'oap_article2type.csv': "SELECT * FROM [oap_article2type.csv]",
            'oap_propedit.csv': "SELECT * FROM [oap_propedit.csv]",
        },
        'GO': {
            'go_types.csv': "SELECT * FROM [go_types.csv]",
            'go_articles.csv': "SELECT * FROM [go_articles.csv]",
        },
    }


if 'sql_commands' not in st.session_state:
    st.session_state['sql_commands'] = default_sql_commands()

with HEADER:
    query_article = st.text_input('Artikel').upper().replace('*', '%')
    query_program = st.text_input('Program').upper().replace('*', '%')

    if query_program and not query_article:
        query_article = '%'

    has_query = (query_article or query_program)

    if st.button('Filtern'):

        if not has_query:
            st.session_state['sql_commands'] = default_sql_commands()

with OCD:
    if has_query:
        st.session_state['sql_commands']['OCD'][
            'ocd_article.csv'] = f"SELECT * FROM [ocd_article.csv] WHERE article_nr LIKE '{query_article}' AND __sql__program__ LIKE '{query_program}';"

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

with OAP:
    # if query_article:
    #     st.session_state['sql_commands']['OAP'][
    #         'oap_metatype2type.csv'] = f"SELECT * FROM [oam_article2ofml.csv] WHERE article LIKE '{query_article}';"
    #
    #     st.session_state['sql_commands']['OAM'][
    #         'oap_article2type.csv'] = f"SELECT * FROM [oap_article2type.csv] WHERE article LIKE '{query_article}';"

    for table_name, sql in st.session_state['sql_commands']['OAP'].items():
        df = pd.read_sql(sql, DB)
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)

with GO:
    # if query_article:
    #     st.session_state['sql_commands']['OAP'][
    #         'oap_metatype2type.csv'] = f"SELECT * FROM [oam_article2ofml.csv] WHERE article LIKE '{query_article}';"
    #
    #     st.session_state['sql_commands']['OAM'][
    #         'oap_article2type.csv'] = f"SELECT * FROM [oap_article2type.csv] WHERE article LIKE '{query_article}';"

    for table_name, sql in st.session_state['sql_commands']['GO'].items():
        df = pd.read_sql(sql, DB)
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)
