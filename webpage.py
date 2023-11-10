import asyncio
import json
from datetime import datetime
import websocket
import pandas as pd
import streamlit as st
from sqlalchemy import text

from db import get_new_connection
import threading

_db_connection = get_new_connection()


def get_local_db_connection():
    global _db_connection
    if _db_connection.closed:
        _db_connection = get_new_connection()
    return _db_connection


def _close_cb_connection():
    global _db_connection
    if _db_connection is None:
        return
    if _db_connection.closed:
        return
    _db_connection.close()


print('- rerurn', threading.current_thread().name, threading.current_thread().ident, id(threading.current_thread()))


def ws_on_open(*args, **kwargs):
    print('WEB :: ws_on_open', args, kwargs)


def ws_on_close(*args, **kwargs):
    print('WEB :: ws_on_close', args, kwargs)


def ws_on_message(*args, **kwargs):
    print('WEB :: ws_on_message', args, kwargs)
    message = args[1]
    message = json.loads(message)
    # print(message)
    st.toast(f'Aktualisiert: {message["program"]}, {message["ofml_part"]}, {message["table"]}')

    # print('rerun?')

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


def get_ofml_features(program_name):
    sql_ocd = f"SELECT 1 FROM ocd_article_csv WHERE __sql__program__='{program_name}';"
    sql_oas = f"SELECT 1 FROM article_csv WHERE __sql__program__='{program_name}';"
    sql_oam = f"SELECT 1 FROM oam_article2ofml_csv WHERE __sql__program__='{program_name}';"
    sql_oap = f"SELECT 1 FROM oap_type_csv WHERE __sql__program__='{program_name}';"
    sql_go = f"SELECT 1 FROM go_articles_csv WHERE __sql__program__='{program_name}';"

    with get_local_db_connection() as db:
        return {
            'OCD': bool(db.execute(text(sql_ocd)).fetchone()),
            'OAS': bool(db.execute(text(sql_oas)).fetchone()),
            'OAM': bool(db.execute(text(sql_oam)).fetchone()),
            'OAP': bool(db.execute(text(sql_oap)).fetchone()),
            'GO': bool(db.execute(text(sql_go)).fetchone()),
        }


with st.sidebar:
    waiting_symbol = st.container()

    sql_all_programs = "SELECT DISTINCT __sql__program__ FROM ocd_article_csv;"  #
    with get_local_db_connection() as db:
        all_programs = db.execute(text(sql_all_programs)).mappings().all()
        all_programs = [_['__sql__program__'] for _ in all_programs]

    st.toggle('Live Änderungen', key='live_changes', value=False)

    st.markdown('## Programmserien')
    ofml_features = {_: get_ofml_features(_) for _ in all_programs}
    for i, program_name in enumerate(all_programs):
        with st.expander(f'**{i + 1}** {program_name}'):
            st.write(program_name)
            features = ofml_features[program_name]
            st.write(features)

HEADER = st.container()
OCD, OAS, OAM, OAP, GO = st.tabs(['OCD', 'OAS', 'OAM', 'OAP', 'GO'])


def default_sql_commands():
    return {
        'OCD': {
            'ocd_article.csv': "SELECT * FROM ocd_article_csv;",
            'ocd_propertyclass.csv': "SELECT * FROM ocd_propertyclass_csv;",
            'ocd_artbase.csv': "SELECT * FROM ocd_artbase_csv;",
            'ocd_articletaxes.csv': "SELECT * FROM ocd_articletaxes_csv;",
            'ocd_artshorttext.csv': "SELECT * FROM ocd_artshorttext_csv;",
            'ocd_artlongtext.csv': "SELECT * FROM ocd_artlongtext_csv;",
        },
        'OAS': {
            'article.csv': "SELECT * FROM article_csv",
            'text.csv': "SELECT * FROM text_csv",
            # 'variant.csv': "SELECT * FROM variant_csv",
            'resource.csv': "SELECT * FROM resource_csv",
            'structure.csv': "SELECT * FROM structure_csv",
        },
        'OAM': {
            'oam_article2ofml.csv': "SELECT * FROM oam_article2ofml_csv",
            'oam_article2odbparams.csv': "SELECT * FROM oam_article2odbparams_csv",
            'oam_property2mat.csv': "SELECT * FROM oam_property2mat_csv",
        },
        'OAP': {
            'oap_metatype2type.csv': "SELECT * FROM oap_metatype2type_csv",
            'oap_article2type.csv': "SELECT * FROM oap_article2type_csv",
            'oap_propedit.csv': "SELECT * FROM oap_propedit_csv",
        },
        'GO': {
            'go_types.csv': "SELECT * FROM go_types_csv",
            'go_articles.csv': "SELECT * FROM go_articles_csv",
        },
    }


if 'sql_commands' not in st.session_state:
    st.session_state['sql_commands'] = default_sql_commands()


def make_article_where_clause(column_name):
    global query_article

    if not query_article:
        query_article = '%'

    query_article = query_article.upper().replace('*', '%')

    query_article = query_article.replace('%', '%%')

    tokens = query_article.split()

    where = ' '.join([f"{column_name} LIKE '{_}' OR" for _ in tokens])[0:-3]
    return where


with HEADER:
    # with st.form(key='article_from'):
    #     query_article = st.text_input('Artikel')
    #     has_query = bool(query_article)
    #
    #     if st.form_submit_button('Filtern'):
    #         # st.snow()
    #
    #         if not has_query:
    #             st.session_state['sql_commands'] = default_sql_commands()

    query_article = st.text_input('Artikel')
    has_query = bool(query_article)

    if not has_query:
        st.session_state['sql_commands'] = default_sql_commands()

if not has_query:
    st.info('Keine Artikel zum filtern angegeben.')
    if st.button('Alles anzeigen?'):
        st.session_state['sql_commands'] = default_sql_commands()
    else:
        st.stop()


with OCD:
    if has_query:
        query_article_clause = make_article_where_clause(column_name='article_nr')
        st.session_state['sql_commands']['OCD'][
            'ocd_article.csv'] = f"SELECT * FROM ocd_article_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OCD'][
            'ocd_propertyclass.csv'] = f"SELECT * FROM ocd_propertyclass_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OCD'][
            'ocd_artbase.csv'] = f"SELECT * FROM ocd_artbase_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OCD'][
            'ocd_articletaxes.csv'] = f"SELECT * FROM ocd_articletaxes_csv WHERE {query_article_clause};"

    for table_name, sql in st.session_state['sql_commands']['OCD'].items():
        df = pd.read_sql(sql, get_local_db_connection())
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.markdown(f'##  {sql}')
        st.write(df)
#
with OAS:
    if query_article:
        query_article_clause = make_article_where_clause(column_name='name')
        st.session_state['sql_commands']['OAS'][
            'article.csv'] = f"SELECT * FROM article_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OAS'][
            'text.csv'] = f"SELECT * FROM text_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OAS'][
            'resource.csv'] = f"SELECT * FROM resource_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OAS'][
            'structure.csv'] = f"SELECT * FROM structure_csv WHERE {query_article_clause};"

    for table_name, sql in st.session_state['sql_commands']['OAS'].items():
        df = pd.read_sql(sql, get_local_db_connection())
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)

with OAM:
    if query_article:
        query_article_clause = make_article_where_clause(column_name='article')
        st.session_state['sql_commands']['OAM'][
            'oam_article2ofml.csv'] = f"SELECT * FROM oam_article2ofml_csv WHERE {query_article_clause};"

        st.session_state['sql_commands']['OAM'][
            'oam_article2odbparams.csv'] = f"SELECT * FROM oam_article2odbparams_csv WHERE {query_article_clause};"

    for table_name, sql in st.session_state['sql_commands']['OAM'].items():
        df = pd.read_sql(sql, get_local_db_connection())
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)
#
with OAP:
    if query_article:
        # st.session_state['sql_commands']['OAP'][
        #     'oap_metatype2type.csv'] = f"SELECT * FROM oap_metatype2type_csv WHERE article_id LIKE '{query_article}';"

        st.session_state['sql_commands']['OAP'][
            'oap_article2type.csv'] = f"SELECT * FROM oap_article2type_csv WHERE article_id LIKE '{query_article}';"

    for table_name, sql in st.session_state['sql_commands']['OAP'].items():
        df = pd.read_sql(sql, get_local_db_connection())
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)
#
with GO:
    if query_article:
        query_article_clause = make_article_where_clause(column_name='article_nr')
        st.session_state['sql_commands']['GO'][
            'go_articles.csv'] = f"SELECT * FROM go_articles_csv WHERE {query_article_clause};"

    for table_name, sql in st.session_state['sql_commands']['GO'].items():
        df = pd.read_sql(sql, get_local_db_connection())
        st.write(f'{table_name} | {df.shape[0]} Einträge')
        # st.write(sql)
        st.write(df)


#####################


# @st.cache_data
def listen_ws_now():
    print('create new ws!')
    url = "ws://127.0.0.1:8765"
    ws = websocket.WebSocketApp(url,
                                on_close=ws_on_close,
                                on_error=ws_on_error,
                                on_open=ws_on_open,
                                on_message=ws_on_message)

    ws.run_forever()
    return ws


_close_cb_connection()
if st.session_state.get('live_changes', None) is True:
    listen_ws_now()

# if 'ws' not in st.session_state:
#     st.session_state['ws'] = 1
#     listen_ws_now()

# asyncio.run(watch(test))
