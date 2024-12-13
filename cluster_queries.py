from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import MinMaxScaler
from umap import UMAP
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

LOAD_LOCATION = './save/processed.csv'
CLUSTER_CENTER_SAVE = './save/cluster.csv'
QUERY_TO_CLUSTER = './save/query_to_cluster.csv'

GET_OPTIMAL_K = False
OPTIMAL_K = 4

# plots silhouette and inertia for k = 2 .. max_k
def plot_optimal_k(x, max_k = 21):
    inertia = []
    silhouette_scores = []
    k_range = range(2, max_k)
    for K in k_range:
        kmeans = KMeans(n_clusters=K, random_state=42)
        kmeans.fit(x)
        inertia.append(kmeans.inertia_) 
        score = silhouette_score(x, kmeans.labels_)
        silhouette_scores.append(score) 

    # Plotting the Elbow method graph
    plt.figure(figsize=(10, 5))
    plt.plot(k_range, inertia, marker='o')
    plt.title('Elbow Method for Optimal K')
    plt.xlabel('Number of Clusters (K)')
    plt.ylabel('Inertia (Sum of squared distances)')

    # Plotting Silhouette Scores for each K
    plt.figure(figsize=(10, 5))
    plt.plot(k_range, silhouette_scores, marker='o')
    plt.title('Silhouette Scores for Different K')
    plt.xlabel('Number of Clusters (K)')
    plt.ylabel('Silhouette Score')
    plt.show()

def heatmap(df, title="Cluster Heatmap"):
    cluster_labels = df['cluster_label']
    df = df[df.columns.difference(['cluster_label'])]
    avg_per_cluster = df.groupby(cluster_labels).mean()
    sns.heatmap(avg_per_cluster.T, cmap='viridis', annot=True)
    plt.tight_layout()
    plt.show()

def min_max_scale(df):
    columns = df.columns
    scaler = MinMaxScaler()
    x = scaler.fit_transform(df)
    df = pd.DataFrame(x, columns=columns)
    return df

def visualize_clusters(df):
    p = sns.scatterplot(df, x='UMAP Dimension-1', y='UMAP Dimension-2', hue='cluster_label', style='cluster_label',
                    palette="deep", s=60)
    # plt.legend(markerscale=3)
    for index in range(0, len(df)):
        p.text(df['UMAP Dimension-1'][index]+0.1, df['UMAP Dimension-2'][index], 
                df['query'][index], horizontalalignment='left', 
                size='medium', color='black')
    plt.show()
    

if __name__ == "__main__":
    print("Starting clustering..")
    processed_df = pd.read_csv(LOAD_LOCATION)
    # need to standardize data
    cluster_df = processed_df.drop('query', axis=1)
    cluster_df = cluster_df.pipe(min_max_scale)
    # if true, plot k vs intertia and silhouette plot
    if GET_OPTIMAL_K:
        plot_optimal_k(cluster_df)
    else:
        # Cluster once
        # print cluster centers
        # generate heatmap
        kmeans = KMeans(n_clusters=OPTIMAL_K, random_state=42)
        kmeans.fit(cluster_df)
        num_comps = 2 # try diff values (< 10)
        umap_reducer = UMAP(n_components=num_comps)
        x = umap_reducer.fit_transform(processed_df[['elapsed_time', 'cpu_avg', 'cpu_max', 'ram_avg', 'swap_avg']])
        columns = [f'UMAP Dimension-{i+1}' for i in range(num_comps)]
        reduced_df = pd.DataFrame(x, columns=columns)
        processed_df['cluster_label'] = kmeans.labels_
        cluster_df['cluster_label'] = kmeans.labels_
        reduced_df['cluster_label'] = kmeans.labels_
        reduced_df['query'] = processed_df['query']

        visualize_clusters(reduced_df)

        # cluster_label_only = processed_df.drop('query', axis=1)
        # avg = cluster_label_only.groupby('cluster_label').agg('mean').reset_index()
        # sns.barplot(avg, y="cpu_avg", x="cluster_label")
        # plt.show()
        # # Cluster centers
        # heatmap(cluster_df)
        # processed_df.to_csv(QUERY_TO_CLUSTER, index=False)
        # cluster_label_only.groupby('cluster_label').agg('mean').reset_index().to_csv(CLUSTER_CENTER_SAVE, index=False)