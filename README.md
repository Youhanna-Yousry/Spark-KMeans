# Spark KMeans Clustering
This project implements the K-Means clustering algorithm using Apache Spark. It reads a dataset of points, clusters them into k groups, and iterates until convergence or a maximum number of iterations is reached.

## 🚀 Features
- Uses Apache Spark for distributed computing.
- Supports customizable k-value and max iterations.
- Implements convergence detection based on centroid movement.
- Saves the final cluster centroids to an output file.

## 🔧 Usage
Run the KMeans Clustering:
`spark-submit --class KMeans --master local[*] your-jar-file.jar <input file> <output file> <k> <max iterations>`

## Arguments:
- <input file> - Path to the input dataset (points in space).
- <output file> - Path to save the final centroids.
- <k> - Number of clusters.
- <max iterations> - Maximum number of iterations before stopping.

## 📜 Algorithm Overview
1. Load the dataset and parse it into Point objects.
2. Select k random initial centroids.
3. Iterate until convergence or max iterations:
4. Assign each point to the nearest centroid.
5. Compute new centroids based on cluster assignments.
6. Check if centroids have converged (i.e., movement is below a threshold).
7. Save final centroids to the output file.

## 📊 Output
The output file will contain the final centroids, representing cluster centers.
