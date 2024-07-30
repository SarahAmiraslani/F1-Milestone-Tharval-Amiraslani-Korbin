# F1-Milestone-Tharval-Amiraslani-Tharval-Korbin

## Introduction

Formula 1, often hailed as the pinnacle of motorsport, combines high-speed competition, cutting-edge technology, and a global fan base. This premier racing series showcases a unique synergy of engineering excellence, driver skill, and strategic depth, making it an intriguing subject for in-depth analysis. This project leverages machine learning to explore the complexities of Formula 1, with a primary focus on predicting top 10 finishes in races — a crucial outcome for securing championship points for drivers and teams. Additionally, we analyze race track characteristics through unsupervised learning, clustering tracks based on factors such as layout, surface, and length.

Historically, Formula 1 analytics has focused on predicting race winners using various statistical and machine learning techniques. These studies often reflect the dominance of certain teams during specific eras, such as Red Bull’s supremacy from 2010 to 2013 and 2021 to the present, and Mercedes’ stronghold from 2013 to 2020. While insightful, this focus on predicting race winners can overlook the nuanced dynamics and strategic elements that contribute to the broader competitive landscape of Formula 1 racing.

Our project aims to broaden the analytical scope by targeting the prediction of top 10 finishes. Securing a place in the top 10 is vital for accumulating championship points, crucial for both drivers and constructors over a season. By shifting our focus to these positions, we aim to capture the intricate competitive interactions and strategic nuances that influence race outcomes beyond the winner’s podium. This approach provides a more comprehensive understanding of performance determinants in Formula 1, addressing both the predictability associated with team dominance and the variability arising from tactical decisions across the grid.

Additionally, our exploration into the unsupervised clustering of race tracks adds another dimension to this analysis. By analyzing track layouts, surfaces, and lengths, we aim to classify tracks into distinct clusters, providing insights into how track characteristics influence race strategies and outcomes.

## Methodology

### Data Collection

We collected historical race data from the Ergast Developer API, which includes information on race results, driver standings, and lap times. The goal is to classify each race outcome into a binary variable indicating whether a driver finishes in the top 10. This focus is driven by the significant impact these positions have on championship standings.

### Models

We employ various models, including Random Forest, Logistic Regression, and Neural Networks, each chosen for its ability to capture different aspects of the complex dynamics influencing Formula 1 race outcomes. These models are trained, validated, and tested on data from the Ergast Developer API, providing a robust foundation for our predictive analysis. Accuracy is our primary metric for evaluating model performance, as it offers clear interpretability for our binary classification task, simplifying the analysis by focusing on the overall goal of identifying drivers and conditions that frequently lead to top 10 finishes.

### Track Clustering

Our primary track clustering algorithm was DBSCAN (Density-Based Spatial Clustering of Applications with Noise). This unsupervised clustering algorithm is known for its efficacy in identifying non-normally distributed clusters of varying shapes and sizes without predefining the number of clusters. The optimal epsilon value, a critical hyperparameter for DBSCAN, was estimated using the Nearest Neighbors algorithm. We adhered to the heuristic of setting n_neighbors to `2*dim - 1`, where dim represents the dimensionality of our feature space, to balance local density estimation and computational feasibility. We determined the optimal epsilon value by evaluating where the distance of the 5th nearest neighbor started to increase exponentially in the K-distance graph.

### Code Structure

The `scripts` directory contains functions that are used throughout the notebooks to load and transform data and to train and test the models. The `notebooks` directory is where you can find the data analysis and modeling used in this project.

## Code

The `scripts` directory contains functions that are used throughout the notebooks to load and transform data and to train and test the models. The `notebooks` directory is where you can find the data analysis and modeling used in this project.

## How to Use

1. Clone the repository:

   ```bash

   git clone https://github.com/yourusername/formula1-predictions-track-clustering.git
   ```

2. Install the required packages.

   ```bash
   pip install -r requirements.txt
   ```

## Contributors

- [Sarah Amiraslani](samirasl@umich.edu)
- [Akshay Tharval](tharval@umich.edu)
- [Sam Kobrin](kobrin@umich.edu)

## Acknowledgements

We would like to thank the Ergast Developer API for providing the data used in this project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
