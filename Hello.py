# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)

import base64

import streamlit as st
import requests
import pandas as pd
import time

st.title("CSR Data Fetcher and Merger")

# Function to fetch CSR data for a given CIN
def fetch_csr_data(cin):
    try:
        url = "https://www.csr.gov.in/content/csr/global/master/home/home/csr-expenditure--geographical-distribution/state/district/company.companyDataAPI.html?cin={}&fy=FY%202020-21".format(cin)
        payload = {}
        headers = {
            'Cookie': 'cookiesession1=678B286D57C1C30AD319826AEA641D02'
        }
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        data = response.json()

        data_company_card = data['cmpny_header']['data']['cmpny_header_data']
        df_card = pd.DataFrame.from_dict(data_company_card)

        data_company_csr = data['cmpny_csr_detail']['data']['cmpny_csr_detail_data']
        df_csr = pd.DataFrame.from_dict(data_company_csr)

        df_csr_head = data['cmpny_csr_header']['data']['cmpny_csr_header_data']
        df_csr_header = pd.DataFrame.from_dict(df_csr_head)

        return df_card, df_csr, df_csr_header
    except Exception as e:
        st.warning(f"Error fetching data for CIN {cin}: {str(e)}")
        return None, None, None

# Streamlit App
uploaded_file = st.file_uploader("Upload Wharton CIN Excel file", type=["xlsx"])

if uploaded_file is not None:
    # Read the uploaded Excel file
    df_cin = pd.read_excel(uploaded_file)

    df_card_final = pd.DataFrame()
    df_csr_final = pd.DataFrame()
    df_csr_head_final = pd.DataFrame()

    # Iterate through each CIN in the uploaded file
    with st.spinner("Fetching and processing data..."):
        for i in df_cin['cin']:
            index_no = df_cin.index[df_cin['cin'] == i].tolist()[0]
            st.write("Index of cin : {}".format( index_no))
            df_card, df_csr, df_csr_header = fetch_csr_data(i)

            if df_card is not None:
                df_card_final = pd.concat([df_card_final, df_card], ignore_index=True)

            if df_csr is not None:
                df_csr_final = pd.concat([df_csr_final, df_csr], ignore_index=True)

            if df_csr_header is not None:
                df_csr_head_final = pd.concat([df_csr_head_final, df_csr_header], ignore_index=True)

            time.sleep(2)  # Add a delay to avoid hitting the API too frequently

    # Merge the DataFrames
    merged_df = pd.merge(df_csr_head_final, df_card_final, on="cin")
    merged_df = pd.merge(merged_df, df_csr_final, on="cin")

    # Display the merged DataFrame
    st.subheader("Merged DataFrame")
    st.dataframe(merged_df)

    if st.button("Download Final Dataset"):
        # Download the DataFrame as a CSV file
        csv = merged_df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # Convert to base64 encoding
        href = f'<a href="data:file/csv;base64,{b64}" download="final_dataset.csv">Download CSV</a>'
        st.markdown(href, unsafe_allow_html=True)


