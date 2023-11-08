import asyncio
import json
from datetime import datetime
import websocket
import pandas as pd
import streamlit as st
from sqlalchemy import text

from db import DB
import threading

print('- rerurn', threading.current_thread().name, threading.current_thread().ident, id(threading.current_thread()))


def ws_on_open(*args, **kwargs):
    print('WEB :: ws_on_open', args, kwargs)


def ws_on_close(*args, **kwargs):
    print('WEB :: ws_on_close', args, kwargs)


def ws_on_message(*args, **kwargs):
    print('WEB :: ws_on_message', args, kwargs)
    message = args[1]
    message = json.loads(message)
    print(message)
    st.toast(f'Aktualisiert: {message["program"]}, {message["ofml_part"]}, {message["table"]}')

    print('rerun?')

    st.rerun()


def ws_on_error(*args, **kwargs):
    print('WEB :: ws_on_error', args, kwargs)


# extend the streamlit browser container window default is 48rem
css = '''
<style>
    #root > div > div > div > div > div > section > div {
    max-width: 108rem;
    }

</style>
'''
st.markdown(css, unsafe_allow_html=True)


# @st.cache_resource
def db_connection():
    return DB


def get_ofml_features(program_name):
    sql_ocd = f"SELECT 1 FROM [ocd_article.csv] WHERE __sql__program__='{program_name}';"
    sql_oas = f"SELECT 1 FROM [article.csv] WHERE __sql__program__='{program_name}';"
    sql_oam = f"SELECT 1 FROM [oam_article2ofml.csv] WHERE __sql__program__='{program_name}';"
    sql_oap = f"SELECT 1 FROM [oap_type.csv] WHERE __sql__program__='{program_name}';"
    sql_go = f"SELECT 1 FROM [go_articles.csv] WHERE __sql__program__='{program_name}';"

    with db_connection() as db:
        crx = db.cursor()
        crx.execute(sql_ocd)
        has_ocd = bool(crx.fetchone())

        crx.execute(sql_oas)
        has_oas = bool(crx.fetchone())

        crx.execute(sql_oam)
        has_oam = bool(crx.fetchone())

        crx.execute(sql_oap)
        has_oap = bool(crx.fetchone())

        crx.execute(sql_go)
        has_go = bool(crx.fetchone())

    return {
        'OCD': has_ocd,
        'OAS': has_oas,
        'OAM': has_oam,
        'OAP': has_oap,
        'GO': has_go,
    }


with st.sidebar:
    waiting_symbol = st.container()

    sql_all_programs = "SELECT DISTINCT __sql__program__ FROM [ocd_article.csv];" #
    with db_connection() as db:
        print('xxxxxxxxxxxxxxxx')
        all_programs = db.execute(text(sql_all_programs)).mappings()
        print(all_programs.all())
        #mappings()

    # st.markdown('## Programmserien')
    # for i, program_name in enumerate(all_programs):
    #     with st.expander(f'**{i+1}** {program_name}'):
    #         st.write(program_name)
            # features = get_ofml_features(program_name)
            # st.write(features)


st.stop()
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
        # 'OAM': {
        #     'oam_article2ofml.csv': "SELECT * FROM [oam_article2ofml.csv]",
        #     'oam_article2odbparams.csv': "SELECT * FROM [oam_article2odbparams.csv]",
        # },
        # 'OAP': {
        #     'oap_metatype2type.csv': "SELECT * FROM [oap_metatype2type.csv]",
        #     'oap_article2type.csv': "SELECT * FROM [oap_article2type.csv]",
        #     'oap_propedit.csv': "SELECT * FROM [oap_propedit.csv]",
        # },
        # 'GO': {
        #     'go_types.csv': "SELECT * FROM [go_types.csv]",
        #     'go_articles.csv': "SELECT * FROM [go_articles.csv]",
        # },
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
        st.markdown(f'##  {sql}')
        st.write(df)
#
# with OAS:
#     if query_article:
#         st.session_state['sql_commands']['OAS'][
#             'article.csv'] = f"SELECT * FROM [article.csv] WHERE name LIKE '{query_article}';"
#
#         st.session_state['sql_commands']['OAS'][
#             'text.csv'] = f"SELECT * FROM [text.csv] WHERE name LIKE '{query_article}';"
#
#     for table_name, sql in st.session_state['sql_commands']['OAS'].items():
#         df = pd.read_sql(sql, DB)
#         st.write(f'{table_name} | {df.shape[0]} Einträge')
#         # st.write(sql)
#         st.write(df)

# with OAM:
#     if query_article:
#         st.session_state['sql_commands']['OAM'][
#             'oam_article2ofml.csv'] = f"SELECT * FROM [oam_article2ofml.csv] WHERE article LIKE '{query_article}';"
#
#         st.session_state['sql_commands']['OAM'][
#             'oam_article2odbparams.csv'] = f"SELECT * FROM [oam_article2odbparams.csv] WHERE article LIKE '{query_article}';"
#
#     for table_name, sql in st.session_state['sql_commands']['OAM'].items():
#         df = pd.read_sql(sql, DB)
#         st.write(f'{table_name} | {df.shape[0]} Einträge')
#         # st.write(sql)
#         st.write(df)
#
# with OAP:
#     # if query_article:
#     #     st.session_state['sql_commands']['OAP'][
#     #         'oap_metatype2type.csv'] = f"SELECT * FROM [oam_article2ofml.csv] WHERE article LIKE '{query_article}';"
#     #
#     #     st.session_state['sql_commands']['OAM'][
#     #         'oap_article2type.csv'] = f"SELECT * FROM [oap_article2type.csv] WHERE article LIKE '{query_article}';"
#
#     for table_name, sql in st.session_state['sql_commands']['OAP'].items():
#         df = pd.read_sql(sql, DB)
#         st.write(f'{table_name} | {df.shape[0]} Einträge')
#         # st.write(sql)
#         st.write(df)
#
# with GO:
#     # if query_article:
#     #     st.session_state['sql_commands']['OAP'][
#     #         'oap_metatype2type.csv'] = f"SELECT * FROM [oam_article2ofml.csv] WHERE article LIKE '{query_article}';"
#     #
#     #     st.session_state['sql_commands']['OAM'][
#     #         'oap_article2type.csv'] = f"SELECT * FROM [oap_article2type.csv] WHERE article LIKE '{query_article}';"
#
#     for table_name, sql in st.session_state['sql_commands']['GO'].items():
#         df = pd.read_sql(sql, DB)
#         st.write(f'{table_name} | {df.shape[0]} Einträge')
#         # st.write(sql)
#         st.write(df)


#####################
test = st.empty()


async def watch(test):
    while True:
        test.markdown(
            f"""
            <p class="time">
                {str(datetime.now())}
            </p>
            """, unsafe_allow_html=True)
        await asyncio.sleep(1)


def listen_ws_now():
    print('create new ws!')
    url = "ws://127.0.0.1:8765"
    ws = websocket.WebSocketApp(url,
                                on_close=ws_on_close,
                                on_error=ws_on_error,
                                on_open=ws_on_open,
                                on_message=ws_on_message)

    ws.run_forever()


if 'ws' not in st.session_state:
    listen_ws_now()
    st.session_state['ws'] = 1
# asyncio.run(watch(test))
