# app.py (V5.0 Final - Refactored & Enhanced)

import streamlit as st
import pandas as pd
import pymongo
import re
import plotly.express as px
from typing import Dict, Any, List

# --- 1. 页面配置与常量定义 ---
st.set_page_config(
    page_title="NJ 仓库工作洞察仪表盘",
    page_icon="🏗️",
    layout="wide"
)

# --- 常量定义 ---
CITY_NORMALIZATION_MAP: Dict[str, str] = {
    'south brunswick township': 'South Brunswick',
    'north brunswick township': 'North Brunswick',
    'edison township': 'Edison',
    'new brunswick city': 'New Brunswick',
    'jersey city': 'Jersey City',
}
KEYWORDS: List[str] = ['Operator', 'Forklift', 'Lead', 'Supervisor', 'Manager', 'Associate', 'Technician', 'Driver',
                       'Picker', 'Packer']
DISPLAY_COLUMNS: List[str] = ['title', 'company', 'city', 'pay_period', 'benefits', 'salary', 'hourly_rate', 'url']
DOWNLOAD_COLUMNS: List[str] = ['title', 'company', 'city', 'original_city', 'pay_period', 'benefits', 'salary',
                               'hourly_rate', 'url']


# --- 2. 数据加载与处理模块 ---

@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    """从 MongoDB 加载数据"""
    try:
        client = pymongo.MongoClient(st.secrets["mongo"]["connection_string"])
        db = client[st.secrets["mongo"]["db_name"]]
        collection = db[st.secrets["mongo"]["collection_name"]]
        data = list(collection.find({}, {"_id": 0}))
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"连接 MongoDB 失败: {e}")
        return pd.DataFrame()


@st.cache_data
def clean_and_process_data(_df: pd.DataFrame) -> pd.DataFrame:
    """对原始 DataFrame 进行完整的清洗、解析和标准化"""
    df = _df.copy()

    def parse_salary_intelligently(salary_str: Any) -> Dict[str, Any]:
        if not isinstance(salary_str, str): return {"hourly_rate": None, "pay_period": "Unknown"}
        s_lower = salary_str.lower().replace(',', '')
        numbers_str = re.findall(r'\d+\.?\d*', s_lower)
        if not numbers_str: return {"hourly_rate": None, "pay_period": "Unknown"}
        avg_salary = sum([float(s) for s in numbers_str]) / len(numbers_str)
        if 'year' in s_lower: return {"hourly_rate": avg_salary / 2080, "pay_period": "Annual"}
        if 'month' in s_lower: return {"hourly_rate": avg_salary / 173.33, "pay_period": "Monthly"}
        if 'week' in s_lower: return {"hourly_rate": avg_salary / 40, "pay_period": "Weekly"}
        if avg_salary > 2000: return {"hourly_rate": avg_salary / 2080, "pay_period": "Annual (Inferred)"}
        return {"hourly_rate": avg_salary, "pay_period": "Hourly"}

    if 'salary' in df.columns:
        salary_data = df['salary'].apply(lambda x: pd.Series(parse_salary_intelligently(x)))
        df = pd.concat([df, salary_data], axis=1)
    else:
        df['hourly_rate'], df['pay_period'] = None, 'Unknown'

    df.dropna(subset=['hourly_rate'], inplace=True)
    df = df[df['hourly_rate'] > 0]
    if df.empty: return pd.DataFrame()

    df['original_city'] = df['location'].apply(lambda x: x.split(',')[0].strip() if pd.notnull(x) else 'Unknown')
    df['city'] = df['original_city'].apply(lambda name: CITY_NORMALIZATION_MAP.get(name.lower(), name))

    if 'benefits' not in df.columns: df['benefits'] = [[] for _ in range(len(df))]
    df['benefits'] = df['benefits'].apply(lambda d: d if isinstance(d, list) else [])

    return df


@st.cache_data
def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    """将 DataFrame 转换为可供下载的 CSV 格式"""
    df_copy = df.copy()
    if 'benefits' in df_copy.columns:
        df_copy['benefits'] = df_copy['benefits'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    return df_copy.to_csv(index=False).encode('utf-8')


# --- 3. UI 界面渲染模块 ---

def display_sidebar(df: pd.DataFrame) -> Dict[str, Any]:
    """渲染侧边栏并返回用户筛选条件"""
    st.sidebar.header("筛选器")

    all_cities = sorted(df['city'].unique())
    city_options = ['-- All Cities --'] + all_cities
    selected_city = st.sidebar.selectbox("选择城市:", city_options)

    pay_period_options = sorted(df['pay_period'].unique())
    selected_pay_periods = st.sidebar.multiselect("选择薪资类型:", pay_period_options, default=pay_period_options)

    title_keyword = st.sidebar.text_input("职位关键词:")

    min_salary, max_salary = float(df['hourly_rate'].min()), float(df['hourly_rate'].max())
    selected_salary_range = st.sidebar.slider("选择换算后时薪范围 ($):", min_value=min_salary, max_value=max_salary,
                                              value=(min_salary, max_salary))

    return {
        "city": selected_city,
        "pay_periods": selected_pay_periods,
        "keyword": title_keyword,
        "salary_range": selected_salary_range
    }


def display_kpis_and_diagnostics(df: pd.DataFrame):
    """显示核心指标和极端值诊断模块"""
    st.markdown("### 核心指标")
    total_jobs = len(df)
    avg_salary = df['hourly_rate'].mean() if total_jobs > 0 else 0
    median_salary = df['hourly_rate'].median() if total_jobs > 0 else 0
    col1, col2, col3 = st.columns(3)
    col1.metric("总岗位数", f"{total_jobs}")
    col2.metric("平均时薪", f"${avg_salary:.2f}")
    col3.metric("时薪中位数", f"${median_salary:.2f}")

    with st.expander("🔍 点击展开，查看极端值诊断分析"):
        if total_jobs > 0:
            outliers_df = df.nlargest(10, 'hourly_rate')
            st.dataframe(outliers_df[['salary', 'pay_period', 'hourly_rate', 'title', 'company', 'city']],
                         use_container_width=True)
        else:
            st.info("当前无数据显示。")


def display_geo_analysis_tab(df: pd.DataFrame, selected_city: str):
    """渲染地理分析选项卡"""
    st.subheader("各城市薪资水平分布")
    if not df.empty:
        fig_box = px.box(df, x='city', y='hourly_rate', color='pay_period', title="薪资范围洞察",
                         labels={'city': '城市', 'hourly_rate': '换算后时薪 ($)', 'pay_period': '薪资类型'})
        if selected_city != '-- All Cities --':
            fig_box.update_xaxes(title_text='', showticklabels=False)
        st.plotly_chart(fig_box, use_container_width=True)
    else:
        st.warning("无数据显示。")


def display_keyword_analysis_tab(df: pd.DataFrame):
    """渲染职位关键词分析选项卡"""
    st.subheader("热门职位关键词分析")
    if not df.empty:
        keyword_data = []
        for keyword in KEYWORDS:
            subset = df[df['title'].str.contains(keyword, case=False)]
            if not subset.empty:
                keyword_data.append(
                    {'关键词': keyword, '岗位数': len(subset), '平均时薪': subset['hourly_rate'].mean()})
        if keyword_data:
            keyword_df = pd.DataFrame(keyword_data).sort_values(by="岗位数", ascending=False)
            fig_keyword = px.bar(keyword_df, x='关键词', y='岗位数', color='平均时薪',
                                 title="热门职位关键词及其平均薪资")
            st.plotly_chart(fig_keyword, use_container_width=True)
        else:
            st.info("在当前筛选结果中未找到常见的职位关键词。")


def display_company_analysis_tab(df: pd.DataFrame):
    """新增：渲染公司分析选项卡"""
    st.subheader("招聘公司分析")
    if not df.empty:
        top_n = st.slider("选择要分析的公司数量:", 5, 25, 10)
        company_counts = df['company'].value_counts().nlargest(top_n)
        top_companies = company_counts.index.tolist()

        company_df = df[df['company'].isin(top_companies)]

        fig_comp = px.box(company_df, x='company', y='hourly_rate', color='company',
                          category_orders={'company': top_companies}, title=f"Top {top_n} 招聘公司的薪资分布",
                          labels={'company': '公司', 'hourly_rate': '换算后时薪 ($)'})
        fig_comp.update_xaxes(tickangle=45)
        st.plotly_chart(fig_comp, use_container_width=True)
    else:
        st.warning("无数据显示。")


def display_data_table(df: pd.DataFrame):
    """渲染数据详情表格和下载按钮"""
    st.subheader("数据详情")
    if not df.empty:
        csv_data = convert_df_to_csv(df[DOWNLOAD_COLUMNS])
        st.download_button(label="📥 下载当前数据 (CSV)", data=csv_data, file_name="warehouse_jobs_filtered.csv",
                           mime='text/csv')
        st.dataframe(df[DISPLAY_COLUMNS])


# --- 4. 主程序入口 ---

def main():
    """主函数，编排整个应用"""
    st.title("🏗️ NJ 仓库工作洞察仪表盘 (V5.0)")

    # 加载和处理数据
    raw_df = load_data()
    if raw_df.empty:
        return  # 如果没有数据则停止

    df = clean_and_process_data(raw_df)
    if df.empty:
        st.error("数据清洗后无有效记录。")
        return

    # 显示侧边栏并获取筛选条件
    filters = display_sidebar(df)

    # 应用筛选
    filtered_df = df.copy()
    if filters["city"] != '-- All Cities --':
        filtered_df = filtered_df[filtered_df['city'] == filters["city"]]

    filtered_df = filtered_df[
        (filtered_df['pay_period'].isin(filters["pay_periods"])) &
        (filtered_df['hourly_rate'] >= filters["salary_range"][0]) &
        (filtered_df['hourly_rate'] <= filters["salary_range"][1])
        ]

    if filters["keyword"]:
        filtered_df = filtered_df[filtered_df['title'].str.contains(filters["keyword"], case=False, na=False)]

    # 显示核心指标和诊断模块
    display_kpis_and_diagnostics(filtered_df)
    st.markdown("---")

    # 使用选项卡组织主要分析内容
    tab1, tab2, tab3, tab4 = st.tabs(["🌍 地理分析", "🔑 职位关键词", "🏢 公司分析", "📋 详细数据"])

    with tab1:
        display_geo_analysis_tab(filtered_df, filters["city"])
    with tab2:
        display_keyword_analysis_tab(filtered_df)
    with tab3:
        display_company_analysis_tab(filtered_df)
    with tab4:
        display_data_table(filtered_df)


if __name__ == "__main__":
    main()