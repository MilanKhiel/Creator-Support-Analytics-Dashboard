import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def setup_plotting_style():
    """Sets up a clean, professional plotting style."""
    sns.set_theme(style="whitegrid")
    plt.rcParams['figure.figsize'] = (10, 6)
    plt.rcParams['axes.titlesize'] = 16
    plt.rcParams['axes.labelsize'] = 12
    plt.rcParams['xtick.labelsize'] = 10
    plt.rcParams['ytick.labelsize'] = 10
    plt.rcParams['legend.fontsize'] = 10
    plt.rcParams['grid.linestyle'] = '--'
    plt.rcParams['grid.alpha'] = 0.6

def save_plot(fig, filename, path="/home/ubuntu/creator_analytics_project/notebooks/images"):
    """Saves a matplotlib figure to a specified path."""
    if not os.path.exists(path):
        os.makedirs(path)
    full_path = os.path.join(path, filename)
    fig.savefig(full_path, bbox_inches='tight', dpi=300)
    plt.close(fig)
    print(f"Plot saved to {full_path}")
    return full_path

def plot_time_series(df, x_col, y_col, title, ylabel, filename):
    """Generates and saves a simple time series plot."""
    setup_plotting_style()
    fig, ax = plt.subplots()
    
    # Ensure x_col is datetime for proper plotting
    df[x_col] = pd.to_datetime(df[x_col])
    
    sns.lineplot(x=x_col, y=y_col, data=df, ax=ax, marker='o')
    
    ax.set_title(title)
    ax.set_xlabel("Month")
    ax.set_ylabel(ylabel)
    ax.tick_params(axis='x', rotation=45)
    
    return save_plot(fig, filename)

def plot_bar_chart(df, x_col, y_col, title, xlabel, ylabel, filename):
    """Generates and saves a simple bar chart."""
    setup_plotting_style()
    fig, ax = plt.subplots()
    
    sns.barplot(x=x_col, y=y_col, data=df, ax=ax, palette="viridis")
    
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis='x', rotation=45)
    
    return save_plot(fig, filename)

def plot_segmented_time_series(df, x_col, y_col, segment_col, title, ylabel, filename):
    """Generates and saves a segmented time series plot."""
    setup_plotting_style()
    fig, ax = plt.subplots()
    
    # Ensure x_col is datetime for proper plotting
    df[x_col] = pd.to_datetime(df[x_col])
    
    sns.lineplot(x=x_col, y=y_col, hue=segment_col, data=df, ax=ax, marker='o')
    
    ax.set_title(title)
    ax.set_xlabel("Month")
    ax.set_ylabel(ylabel)
    ax.tick_params(axis='x', rotation=45)
    ax.legend(title=segment_col, bbox_to_anchor=(1.05, 1), loc='upper left')
    
    return save_plot(fig, filename)

def plot_retention_heatmap(retention_df, title, filename):
    """Generates and saves a heatmap for cohort retention analysis."""
    setup_plotting_style()
    
    # Pivot the data for heatmap
    retention_pivot = retention_df.pivot(index='cohort_month', columns='month_number', values='retention_rate_pct')
    
    # Rename columns to be more descriptive
    retention_pivot.columns = [f'Month {i}' for i in retention_pivot.columns]
    
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.heatmap(
        retention_pivot,
        annot=True,
        fmt=".1f",
        cmap="Blues",
        cbar_kws={'label': 'Retention Rate (%)'},
        linewidths=.5,
        linecolor='lightgray',
        ax=ax
    )
    
    ax.set_title(title)
    ax.set_ylabel("Cohort (Signup Month)")
    ax.set_xlabel("Membership Month")
    
    return save_plot(fig, filename)

def calculate_cohort_retention(memberships_df):
    """
    Calculates cohort retention using Pandas for robust date handling.
    """
    # 1. Determine the cohort (start month) for each membership
    memberships_df['start_date'] = pd.to_datetime(memberships_df['start_date'])
    memberships_df['cohort_month'] = memberships_df['start_date'].dt.to_period('M')
    
    # 2. Calculate the total number of members in each cohort (Cohort Size)
    cohort_sizes = memberships_df.groupby('cohort_month')['fan_id'].nunique().reset_index()
    cohort_sizes.rename(columns={'fan_id': 'cohort_size'}, inplace=True)
    
    # 3. Create a list of all active months for each membership
    all_active_months = []
    for _, row in memberships_df.iterrows():
        end_date = pd.to_datetime(row['end_date']) if row['end_date'] else pd.to_datetime('2025-06-30')
        start_date = pd.to_datetime(row['start_date'])
        
        if end_date >= start_date:
            months = pd.date_range(start=start_date, end=end_date, freq='MS').to_period('M')
            for m in months:
                all_active_months.append({
                    'fan_id': row['fan_id'],
                    'cohort_month': row['cohort_month'],
                    'active_month': m
                })
    
    membership_months = pd.DataFrame(all_active_months)
    
    # Drop duplicates to count unique fans per month
    membership_months = membership_months.drop_duplicates(subset=['fan_id', 'active_month'])
    
    # 4. Calculate the month number relative to the cohort month
    membership_months['month_number'] = (
        membership_months['active_month'].dt.to_timestamp() - 
        membership_months['cohort_month'].dt.to_timestamp()
    ).apply(lambda x: x.days // 30).astype(int)
    
    # 5. Count the number of retained fans per cohort and month number
    retention_counts = membership_months.groupby(['cohort_month', 'month_number'])['fan_id'].nunique().reset_index()
    retention_counts.rename(columns={'fan_id': 'retained_fans'}, inplace=True)
    
    # 6. Merge with cohort size and calculate retention rate
    retention_df = pd.merge(retention_counts, cohort_sizes, on='cohort_month')
    retention_df['retention_rate_pct'] = (retention_df['retained_fans'] / retention_df['cohort_size']) * 100
    
    # Format cohort_month for display
    retention_df['cohort_month'] = retention_df['cohort_month'].astype(str)
    
    return retention_df
