import pandas as pd
import matplotlib.pyplot as plt
import os
import seaborn as sns
from pathlib import Path
import re

def simplify_filename(filename):
    output_filename = filename.split('_')[2] + ' ' + filename.split('_')[4]
    return output_filename

def create_plot(data, title, filename):
    plt.figure(figsize=(15, 8))
    ax = sns.barplot(data=data, x='simplified_name', y='return', 
                    color='skyblue', width=0.7)
    plt.title(title, fontsize=14)
    plt.xlabel('策略組合', fontsize=12)
    plt.ylabel('平均報酬率', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=6)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.close()

def plot_backtest_returns():
    # 設定字體以正確顯示中文
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
    plt.rcParams['axes.unicode_minus'] = False

    # 儲存結果的列表
    results = []
    
    # 處理 top10 和 bottom10 的資料夾
    folders = ['output_CAPM_results_top10', 'output_CAPM_results_bottom10']
    
    for folder in folders:
        # 遍歷資料夾中的所有 xlsx 檔案
        folder_path = Path(folder)
        for file in folder_path.glob('*.xlsx'):
            try:
                # 讀取 summary 工作表
                df = pd.read_excel(file, sheet_name='summary')
                
                # 取得平均報酬率並確保是數值型態
                if '平均報酬率' in df.columns:
                    avg_return = df.iloc[0]['平均報酬率']
                    # 如果是字串，嘗試移除百分比符號並轉換為浮點數
                    if isinstance(avg_return, str):
                        avg_return = float(avg_return.strip('%')) / 100
                    else:
                        avg_return = float(avg_return)
                    
                    # 儲存檔名和報酬率，使用簡化的檔名
                    results.append({
                        'filename': file.stem,
                        'simplified_name': simplify_filename(file.stem),
                        'return': avg_return,
                        'group': 'top10' if 'top10' in folder else 'bottom10'
                    })

            except Exception as e:
                print(f"處理檔案 {file} 時發生錯誤: {e}")
                print(f"報酬率值: {avg_return}, 類型: {type(avg_return)}")
    
    # 轉換為 DataFrame
    results_df = pd.DataFrame(results)
    
    if not results_df.empty:
        # 分離資料
        top10_df = results_df[results_df['group'] == 'top10']
        bottom10_df = results_df[results_df['group'] == 'bottom10']
        
        # 進一步分離正負報酬率
        top10_positive = top10_df[top10_df['return'] > 0].sort_values('return', ascending=False)
        top10_negative = top10_df[top10_df['return'] <= 0].sort_values('return', ascending=False)
        bottom10_positive = bottom10_df[bottom10_df['return'] > 0].sort_values('return', ascending=False)
        bottom10_negative = bottom10_df[bottom10_df['return'] <= 0].sort_values('return', ascending=False)
        
        # 繪製四張圖
        create_plot(top10_positive, 'Top 10 正報酬率策略組合比較', 'top10_positive_returns.png')
        create_plot(top10_negative, 'Top 10 負報酬率策略組合比較', 'top10_negative_returns.png')
        create_plot(bottom10_positive, 'Bottom 10 正報酬率策略組合比較', 'bottom10_positive_returns.png')
        create_plot(bottom10_negative, 'Bottom 10 負報酬率策略組合比較', 'bottom10_negative_returns.png')
        
        # 輸出統計資訊
        print(f"Top 10 正報酬率策略數量: {len(top10_positive)}")
        print(f"Top 10 負報酬率策略數量: {len(top10_negative)}")
        print(f"Bottom 10 正報酬率策略數量: {len(bottom10_positive)}")
        print(f"Bottom 10 負報酬率策略數量: {len(bottom10_negative)}")
        
        # 找出各組的最高報酬率
        if not top10_positive.empty:
            max_top10_positive = top10_positive.iloc[0]
            print(f"\nTop 10 最高正報酬率策略: {max_top10_positive['filename']}, 報酬率: {max_top10_positive['return']*100:.2f}%")
        
        if not bottom10_positive.empty:
            max_bottom10_positive = bottom10_positive.iloc[0]
            print(f"Bottom 10 最高正報酬率策略: {max_bottom10_positive['filename']}, 報酬率: {max_bottom10_positive['return']*100:.2f}%")
        
        # 找出各組的最低報酬率
        if not top10_negative.empty:
            min_top10_negative = top10_negative.iloc[-1]
            print(f"Top 10 最低負報酬率策略: {min_top10_negative['filename']}, 報酬率: {min_top10_negative['return']*100:.2f}%")
        
        if not bottom10_negative.empty:
            min_bottom10_negative = bottom10_negative.iloc[-1]
            print(f"Bottom 10 最低負報酬率策略: {min_bottom10_negative['filename']}, 報酬率: {min_bottom10_negative['return']*100:.2f}%")
        
    else:
        print("沒有找到有效的資料")

if __name__ == "__main__":
    plot_backtest_returns()
