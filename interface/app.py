from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
from common.csv_input import load_csv, messages
from common.broker import producer, publish
from common.database import results

st.set_page_config(page_title='Скоринг транзакций', layout='wide')
st.title('Скоринг фродовых транзакций')
send_tab, result_tab = st.tabs(['Отправить транзакции', 'Результаты'])
with send_tab:
    st.write('Модель обучена на train.csv соревнования. Здесь CSV имитирует поток транзакций Kafka.')
    upload = st.file_uploader('Загрузите test.csv (читаются первые 10 000 строк)', type=['csv'])
    example = st.selectbox('Встроенный CSV', ['test_sample.csv', 'results_demo.csv'])
    raw = upload.getvalue() if upload else Path('examples', example).read_bytes()
    try:
        frame = load_csv(raw)
        st.dataframe(frame.head(10), hide_index=True)
        count = st.number_input('Количество транзакций', min_value=1, max_value=len(frame), value=min(100,len(frame)))
        if st.button('Отправить в Kafka'):
            sent = 0
            try:
                batch = messages(frame.iloc[:int(count)], raw)
                client = producer()
                progress = st.progress(0)
                for item in batch:
                    publish(client, 'transactions', item, item['transaction_id'])
                    sent += 1
                    progress.progress(sent / len(batch))
                st.success(f'Kafka приняла {sent} сообщений. Откройте результаты после обработки.')
            except Exception as error:
                st.error(f'Подтверждено сообщений: {sent}. Ошибка: {error}')
    except Exception as error:
        st.error(f'Ошибка CSV: {error}')
with result_tab:
    if st.button('Посмотреть результаты'):
        try:
            st.session_state['results'] = (*results(), datetime.now().isoformat(timespec='seconds'))
        except Exception as error:
            st.error(f'Не удалось прочитать PostgreSQL: {error}')
    if 'results' in st.session_state:
        fraud, scores, total, timestamp = st.session_state['results']
        st.caption(f'Обновлено: {timestamp}. Всего записей: {total}')
        st.subheader('10 последних транзакций с fraud_flag = 1')
        if fraud:
            st.dataframe(pd.DataFrame(fraud, columns=['transaction_id','score','fraud_flag','created_at']), hide_index=True)
        else:
            st.info('Фродовых транзакций пока нет.')
        st.subheader(f'Распределение скоров последних {len(scores)} транзакций')
        if scores:
            counts, edges = np.histogram(scores, bins=np.linspace(0,1,11))
            labels = [f'{edges[i]:.1f}–{edges[i+1]:.1f}' for i in range(10)]
            st.bar_chart(pd.DataFrame({'Число транзакций':counts}, index=labels))
        else:
            st.info('Результатов пока нет.')
